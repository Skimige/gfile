import concurrent.futures
import concurrent.futures.thread
import functools
import io
import math
import re
import threading
import time
import uuid
from datetime import datetime
from os import rename
from pathlib import Path
from subprocess import run
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests_toolbelt import MultipartEncoder, StreamingIterator
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from urllib3.util.retry import Retry


def bytes_to_size_str(bytes):
   if bytes == 0:
       return "0B"
   units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")
   i = int(math.floor(math.log(bytes, 1024)))
   p = math.pow(1024, i)
   return f"{bytes/p:.02f} {units[i]}"


def size_str_to_bytes(size_str):
    if isinstance(size_str, int):
        return size_str
    m = re.search(r'^(?P<num>\d+) ?((?P<unit>[KMGTPEZY]?)(iB|B)?)$', size_str, re.IGNORECASE)
    assert m
    units = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
    unit = (m['unit'] or 'B').upper()
    return int(math.pow(1024, units.index(unit)) * int(m['num']))


def filename_from_content_disposition(value):
    """Extract the real filename from a Content-Disposition header.

    gigafile's download page (#dl) often shows a masked name (e.g. ******.bin)
    and may not be rendered right after upload, but download.php always returns
    the true filename in this header, so it's the authoritative source.
    """
    if not value:
        return None
    # RFC 5987 form is preferred: filename*=UTF-8''<percent-encoded>
    m = re.search(r"filename\*\s*=\s*[^']*''([^;]+)", value, re.IGNORECASE)
    if m:
        return unquote(m.group(1)).strip()
    m = re.search(r'filename\s*=\s*"([^"]+)"', value, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r'filename\s*=\s*([^;]+)', value, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def requests_retry_session(
    retries=5,
    backoff_factor=0.2,
    status_forcelist=None, # (500, 502, 504)
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def make_progress(console):
    """Create a rich Progress with a column layout shared by upload/download."""
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        DownloadColumn(binary_units=True),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    )


def split_file(input_file, out, target_size=None, start=0, chunk_copy_size=1024*1024):
    input_file = Path(input_file)
    size = 0

    input_size = input_file.stat().st_size
    if target_size is None:
        output_size = input_size - start
    else:
        output_size = min(target_size, input_size - start)

    with open(input_file, 'rb') as f:
        f.seek(start)
        while True:
            if size == output_size: break
            if size > output_size:
                raise Exception(f'Size ({size}) is larger than {target_size} bytes!')
            current_chunk_size = min(chunk_copy_size, output_size - size)
            chunk = f.read(current_chunk_size)
            if not chunk: break
            size += len(chunk)
            out.write(chunk)


class GFile:
    def __init__(self, file_or_url, progress=False, thread_num=4, chunk_size=1024*1024*30, chunk_copy_size=1024*1024, timeout=30,
                 aria2=False, key=None, mute=False, verify=True, max_retries=10, proxy=None, window=2, **kwargs) -> None:
        self.file_or_url = file_or_url
        self.chunk_size = size_str_to_bytes(chunk_size)
        self.chunk_copy_size = size_str_to_bytes(chunk_copy_size)
        self.thread_num=thread_num
        # in-flight window: how many chunks may normally be uploading
        # concurrently. See the admission gate in upload_chunk for the exact
        # semantics (it is soft in one direction to avoid idle bandwidth).
        self.window = max(1, window)
        self.progress = progress
        self.data = None
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests_retry_session()
        # (connect, read) timeout: fail fast on connect, be patient on read for flaky proxies.
        self.session.request = functools.partial(self.session.request, timeout=(min(10, timeout), timeout))
        if proxy:
            self.session.proxies = {'http': proxy, 'https': proxy}
        self.cookies = None
        self.current_chunk = 0
        # coordinates chunk admission, sender accounting and in-order commits
        self._cond = threading.Condition()
        self.next_send = 0
        self.active_senders = 0
        self.aria2 = aria2
        self.mute = mute
        self.verify = verify
        self.key = key
        self.console = Console()
        # progress state (set up per upload/download run)
        self._progress = None
        self.tasks = None
        self.total_task = None


    def _info(self, msg):
        # informational output, suppressed when muted
        if not self.mute:
            self.console.print(msg)

    def _warn(self, msg):
        if not self.mute:
            self.console.print(msg, style='yellow')

    def _err(self, msg):
        # errors are always shown, even when muted
        self.console.print(msg, style='bold red')


    def _resolve_filename(self, header_name, web_name, output, idx, total):
        # user-provided output always wins; otherwise prefer the authoritative
        # header name, falling back to the (possibly masked) scraped page name.
        if output:
            return output + f'_{idx}' if total > 1 else output
        return re.sub(r'[\\/:*?"<>|]', '_', header_name or web_name)


    def upload_chunk(self, chunk_no, chunks):
        task_id = self.tasks[chunk_no % self.thread_num] if self.tasks else None
        # admission gate: chunks enter the wire strictly in order, normally at
        # most `window` ahead of the commit frontier, so a parked chunk (see
        # the tail-hold in gen below) never idles long enough for the server
        # to drop its connection. The window is soft in one direction: when
        # every in-flight chunk has finished sending and is parked waiting to
        # commit, the next chunk is admitted early so the link never sits idle
        # while the commit chain drains. In-flight chunks -- and thus memory,
        # at ~chunk_size each -- are capped by the worker pool (thread_num).
        with self._cond:
            while not self.failed and not (chunk_no == self.next_send and (
                    chunk_no - self.current_chunk < self.window or self.active_senders == 0)):
                self._cond.wait(0.05)
            if self.failed:
                return
            self.next_send += 1
            self.active_senders += 1
        sender = True

        def become_sender():
            nonlocal sender
            if not sender:
                with self._cond:
                    self.active_senders += 1
                sender = True

        def release_sender():
            nonlocal sender
            if sender:
                with self._cond:
                    self.active_senders -= 1
                    self._cond.notify_all()
                sender = False

        try:
            with io.BytesIO() as f:
                split_file(self.file_or_url, f, self.chunk_size, start=chunk_no * self.chunk_size, chunk_copy_size=self.chunk_copy_size)
                raw_size = f.tell()
                f.seek(0)
                fields = {
                    "id": self.token,
                    "name": Path(self.file_or_url).name,
                    "chunk": str(chunk_no),
                    "chunks": str(chunks),
                    "lifetime": "100",
                    "file": ("blob", f, "application/octet-stream"),
                }
                form_data = MultipartEncoder(fields)
                headers = {
                    "content-type": form_data.content_type,
                }
                # convert the form-data into a binary string, this way we can control/throttle its read() behavior
                form_data_binary = form_data.to_string()
                del form_data

            size = len(form_data_binary)
            update_tick = 1024 * 128
            # gigafile commits chunks strictly in ascending order -- an
            # out-of-order completion corrupts the file *silently* -- so the
            # final piece of the body is held back until every earlier chunk
            # has committed. Completion order is thus guaranteed by
            # construction rather than by how the concurrent uploads happen to
            # race, and a parked chunk needs only one tick to commit once its
            # turn arrives.
            bulk_end = max(0, size - update_tick)

            def set_state(state=''):
                if task_id is not None:
                    desc = f'chunk {chunk_no + 1}/{chunks}'
                    if state:
                        desc += f' [dim]({state})[/]'
                    self._progress.update(task_id, description=desc)

            def reset_bar():
                if task_id is not None:
                    self._progress.reset(task_id, total=size)
                set_state()

            def gen():
                offset = 0
                while offset < bulk_end:
                    if self.failed:
                        return
                    piece_end = min(offset + update_tick, bulk_end)
                    yield form_data_binary[offset:piece_end]
                    if task_id is not None:
                        self._progress.update(task_id, advance=piece_end - offset)
                    offset = piece_end
                # bulk is on the wire; stop counting as a sender so the gate
                # can admit the next chunk while this one waits for its turn.
                release_sender()
                if chunk_no != self.current_chunk:
                    set_state('waiting turn')
                with self._cond:
                    while not self.failed and chunk_no != self.current_chunk:
                        self._cond.wait(0.05)
                if self.failed:
                    return
                yield form_data_binary[offset:]
                if task_id is not None:
                    self._progress.update(task_id, advance=size - offset)
                # resumed here means the tail has been handed to the socket:
                # the body is fully sent and we are waiting on the server.
                set_state('waiting response')

            reset_bar()

            resp_data = None
            for attempt in range(self.max_retries):
                if self.failed:
                    return
                become_sender()
                try:
                    streamer = StreamingIterator(size, gen())
                    resp = self.session.post(f"https://{self.server}/upload_chunk.php", data=streamer, headers=headers)
                    resp_data = resp.json()
                except Exception as ex:
                    # if the upload was cancelled/aborted elsewhere, the failure is
                    # just the truncated request unwinding -- bail quietly, no retry.
                    if self.failed:
                        return
                    # not transmitting during the backoff; let others send.
                    release_sender()
                    wait = min(2 ** attempt, 30)
                    self._warn(f'chunk {chunk_no + 1}/{chunks} failed: {ex} Retrying in {wait}s ({attempt + 1}/{self.max_retries})...')
                    time.sleep(wait)
                    # the whole chunk gets re-sent, so rewind this worker's bar.
                    reset_bar()
                else:
                    break
            else:
                self._err(f'ERROR: chunk {chunk_no + 1}/{chunks} failed after {self.max_retries} attempts.')
                self.failed = True
                return

            with self._cond:
                self.current_chunk += 1
                self._cond.notify_all()
            set_state('done')

            if 'url' in resp_data:
                self.data = resp_data
            if 'status' not in resp_data or resp_data['status']:
                self._err(str(resp_data))
                self.failed = True
                return

            # advance the overall progress by the raw (pre-encoding) size of this chunk
            if self.total_task is not None:
                self._progress.update(self.total_task, advance=raw_size)
        finally:
            release_sender()


    def upload(self):
        self.token = uuid.uuid1().hex
        self.failed = False
        self.current_chunk = 0
        self.next_send = 0
        self.active_senders = 0
        self._progress = None
        self.tasks = None
        self.total_task = None
        assert Path(self.file_or_url).exists()
        size = Path(self.file_or_url).stat().st_size
        chunks = math.ceil(size / self.chunk_size)
        self._info(f'Filesize {bytes_to_size_str(size)}, chunk size: {bytes_to_size_str(self.chunk_size)}, total chunks: {chunks}')

        self.server = re.search(r'var server = "(.+?)"', self.session.get('https://gigafile.nu/').text)[1]

        if self.progress:
            self._progress = make_progress(self.console)
            self.total_task = self._progress.add_task('[cyan]Total', total=size)
            self.tasks = [self._progress.add_task('waiting', total=1) for _ in range(self.thread_num)]

        # managed manually (not via `with`) so the cancel path can avoid the
        # blocking shutdown(wait=True) that `__exit__` would otherwise perform.
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_num)
        futures = {}
        try:
            if self._progress:
                self._progress.start()
            # upload the first chunk to set cookies properly.
            self.upload_chunk(0, chunks)

            # upload second to second last chunk(s)
            if not self.failed:
                futures = {executor.submit(self.upload_chunk, i, chunks): i for i in range(1, chunks)}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as ex:
                        self.failed = True
                        self._err(f'ERROR: unexpected exception in worker: {ex}')
                    if self.failed:
                        for fut in futures:
                            fut.cancel()
                        break
        except KeyboardInterrupt:
            self.failed = True
            self._warn('\nUpload cancelled by user.')
            for fut in futures:
                fut.cancel()
            raise  # let main() report it with a clean exit code
        finally:
            if self.failed:
                # don't block on workers stuck in network I/O; abandon the
                # in-flight POSTs and skip the atexit thread join so the
                # process can exit immediately and cleanly.
                executor.shutdown(wait=False)
                concurrent.futures.thread._threads_queues.clear()
            else:
                # workers are already idle here, so this returns right away.
                executor.shutdown(wait=True)
            if self._progress:
                # drop the per-worker bars, keep only the overall one in the final render.
                for tid in (self.tasks or []):
                    self._progress.remove_task(tid)
                self._progress.stop()

        if self.failed:
            self._err('ERROR: Upload failed!')
            return self
        if not self.data or 'url' not in self.data:
            self._err(f'ERROR: Something went wrong and upload failed. Returned data: {self.data}')
            return self
        self.data['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return self # for chain


    def get_download_page(self):
        if not self.data or 'url' not in self.data:
            return
        uploaded_url = self.data['url']

        f = Path(self.file_or_url)
        f_size = f.stat().st_size
        if self.verify:
            self._info(f"Check if the file {f.name} is uploaded successfully...")
            files_info = self.parse_download_page(uploaded_url)
            file_id = files_info[0][2]
            download_url = uploaded_url.rsplit('/', 1)[0] + '/download.php?file=' + file_id
            with self.session.get(download_url, stream=True) as r:
                uploaded_size = int(r.headers.get('Content-Length', 0))
            if not uploaded_size or uploaded_size != f_size:
                self._err(f"ERROR: File size of {f.name} at {uploaded_url} mismatches: expected {f_size}, got {uploaded_size}.")
                self._err('This means the upload is corrupted. Please try again.')
                return
            self._info(f"[green]Uploaded file {f.name} is verified successfully.")

        self.console.print(f"Finished at {self.data['finished_at']}, filename: {f.name}, size: {bytes_to_size_str(f_size)}")
        self.console.print(uploaded_url)
        return uploaded_url


    def parse_download_page(self, url):
        m = re.search(r'^https?:\/\/\d+?\.gigafile\.nu\/([a-z0-9-]+)$', url)
        if not m:
            self._err(f'ERROR: Invalid URL: {url}. It should be a valid gigafile URL.')
            return
        r = self.session.get(url) # setup cookie
        files_info = []
        try:
            soup = BeautifulSoup(r.text, 'html.parser')
            if soup.select_one('#contents_matomete'):
                self._info('Matomete page (multiple files). Files will be downloaded one by one.')
                for ele in soup.select('.matomete_file'):
                    web_name = ele.select_one('.matomete_file_info > span:nth-child(2)').text.strip()
                    file_id = re.search(r'download\(\d+, *\'(.+?)\'', ele.select_one('.download_panel_btn_dl')['onclick'])[1]
                    size_str = re.search(r'（(.+?)）', ele.select_one('.matomete_file_info > span:nth-child(3)').text.strip())[1]
                    files_info.append((web_name, size_str, file_id))
            else:
                file_id = m[1]
                size_str = soup.select_one('.dl_size').text.strip()
                web_name = soup.select_one('#dl').text.strip()
                files_info.append((web_name, size_str, file_id))
        except Exception as ex:
            self._err(f'ERROR: Failed to parse the page {url}.')
            self._err(str(ex))
            self._err('Please report it back to the developer.')
            return
        if len(files_info) > 1:
            self._info(f'Found {len(files_info)} files in the page.')
        return files_info


    def download(self, output=None):
        downloaded = []
        files_info = self.parse_download_page(self.file_or_url)
        if not files_info:
            return downloaded
        total = len(files_info)
        for idx, (web_name, size_str, file_id) in enumerate(files_info, 1):
            download_url = self.file_or_url.rsplit('/', 1)[0] + '/download.php?file=' + file_id
            if self.key:
                download_url += f'&dlkey={self.key}'

            if self.aria2:
                header_name = None
                if not output:
                    # download.php doesn't answer HEAD, so peek at the headers via a
                    # streaming GET and close it immediately without reading the body.
                    with self.session.get(download_url, stream=True) as r:
                        header_name = filename_from_content_disposition(r.headers.get('Content-Disposition'))
                filename = self._resolve_filename(header_name, web_name, output, idx, total)
                self._info(f'Name: {filename}, size: {size_str}, id: {file_id}')
                cookie_str = "; ".join([f"{cookie.name}={cookie.value}" for cookie in self.session.cookies])
                cmd = ['aria2c', download_url, '--header', f'Cookie: {cookie_str}', '-o', filename]
                cmd.extend(self.aria2.split(' '))
                run(cmd)
                continue

            with self.session.get(download_url, stream=True) as r:
                r.raise_for_status()
                filesize = int(r.headers['Content-Length'])
                header_name = filename_from_content_disposition(r.headers.get('Content-Disposition'))
                filename = self._resolve_filename(header_name, web_name, output, idx, total)
                temp = filename + '.dl'
                self._info(f'Name: {filename}, size: {size_str}, id: {file_id}')
                progress = None
                task_id = None
                if self.progress:
                    desc = filename if len(filename) <= 20 else filename[0:11] + '..' + filename[-7:]
                    progress = make_progress(self.console)
                    task_id = progress.add_task(desc, total=filesize)
                    progress.start()
                try:
                    with open(temp, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=self.chunk_copy_size):
                            f.write(chunk)
                            if progress is not None:
                                progress.update(task_id, advance=len(chunk))
                finally:
                    if progress is not None:
                        progress.stop()

            filesize_downloaded = Path(temp).stat().st_size
            if filesize == filesize_downloaded:
                self.console.print(f'[green]Filesize check passed ({bytes_to_size_str(filesize)}). Succeeded.')
                rename(temp, filename)
            else:
                self._err(f'ERROR: Downloaded file is corrupt (expected {filesize}, got {filesize_downloaded}). '
                          f'Please check the broken file at {temp} and delete it yourself if needed.')
            downloaded.append(filename)
        return downloaded
