import concurrent.futures
import concurrent.futures.thread
import functools
import io
import json
import math
import os
import queue
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

PRIMARY_DOMAIN = 'gigafile.jp'


def normalize_gigafile_url(url):
    """Normalize a gigafile URL to use the primary domain (.jp)."""
    return re.sub(
        r'^(https?://\d+)\.gigafile\.nu(?=/)',
        rf'\1.{PRIMARY_DOMAIN}',
        url,
        flags=re.IGNORECASE,
    )


DOWNLOAD_BLOCK_SIZE = 16 * 1024 * 1024


class RangeResponseError(RuntimeError):
    pass


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


def parse_content_range(value):
    """Return the inclusive byte range and total size from Content-Range."""
    if not value:
        return None
    match = re.fullmatch(r'bytes\s+(\d+)-(\d+)/(\d+)', value.strip(), re.IGNORECASE)
    if not match:
        return None
    return tuple(int(part) for part in match.groups())


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
            if size == output_size:
                break
            if size > output_size:
                raise Exception(f'Size ({size}) is larger than {target_size} bytes!')
            current_chunk_size = min(chunk_copy_size, output_size - size)
            chunk = f.read(current_chunk_size)
            if not chunk:
                break
            size += len(chunk)
            out.write(chunk)


class GFile:
    def __init__(self, file_or_url, progress=False, thread_num=4, chunk_size=1024*1024*30, chunk_copy_size=1024*1024, timeout=30,
                 aria2=False, key=None, mute=False, verify=True, max_retries=10, proxy=None, window=2,
                 download_threads=4, **kwargs) -> None:
        if isinstance(file_or_url, str):
            file_or_url = normalize_gigafile_url(file_or_url)
        self.file_or_url = file_or_url
        self.chunk_size = size_str_to_bytes(chunk_size)
        self.chunk_copy_size = size_str_to_bytes(chunk_copy_size)
        self.thread_num=thread_num
        # Target number of chunks actively sending their bulk data. Chunks
        # parked for an in-order commit no longer occupy a sender slot.
        self.window = max(1, window)
        self.download_threads = max(1, int(download_threads))
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
        # Chunks start in order, with `window` bulk senders kept active when
        # workers are available. A chunk parked at the tail-hold below gives
        # up its sender slot immediately, so a faster later chunk cannot leave
        # the connection underused while an earlier chunk is still sending.
        # Parked chunks still occupy workers and memory, both capped by
        # thread_num.
        with self._cond:
            while not self.failed and not (
                    chunk_no == self.next_send and self.active_senders < self.window):
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
                # Refill this sender slot while the chunk waits for its turn.
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

        self.server = re.search(r'var server = "(.+?)"', self.session.get(f'https://{PRIMARY_DOMAIN}/').text)[1]

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
        uploaded_url = normalize_gigafile_url(self.data['url'])

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
        m = re.search(r'^https?:\/\/(\d+?)\.gigafile\.(?:jp|nu)\/([a-z0-9-]+)$', url)
        if not m:
            self._err(f'ERROR: Invalid URL: {url}. It should be a valid gigafile URL.')
            return
        url = normalize_gigafile_url(url)
        file_id = m[2]
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


    @staticmethod
    def _download_part_ranges(filesize, part_count):
        base_size, extra = divmod(filesize, part_count)
        start = 0
        ranges = []
        for index in range(part_count):
            part_size = base_size + (1 if index < extra else 0)
            ranges.append((start, start + part_size - 1))
            start += part_size
        return ranges


    @staticmethod
    def _download_state_path(temp):
        return Path(f'{temp}.json')


    @staticmethod
    def _new_download_state(file_id, filesize, validator, completed):
        return {
            'version': 2,
            'file_id': file_id,
            'filesize': filesize,
            'validator': validator,
            'completed': completed,
        }


    def _legacy_download_parts(self, temp):
        temp_path = Path(temp)
        part_pattern = re.compile(re.escape(temp_path.name) + r'\.part\d+$')
        paths = []
        if temp_path.parent.exists():
            for path in temp_path.parent.iterdir():
                if path.is_file() and part_pattern.fullmatch(path.name):
                    paths.append(path)
        return sorted(paths, key=lambda path: int(path.suffix[len('.part'):]))


    def _cleanup_download_state(self, temp, legacy_parts=False):
        state_path = self._download_state_path(temp)
        for path in (state_path, Path(f'{state_path}.tmp')):
            if path.exists():
                path.unlink()
        if legacy_parts:
            for path in self._legacy_download_parts(temp):
                path.unlink()


    @staticmethod
    def _normalize_completed(ranges, filesize):
        normalized = []
        for item in ranges:
            if (
                    not isinstance(item, (list, tuple))
                    or len(item) != 2
                    or type(item[0]) is not int
                    or type(item[1]) is not int
                    or not 0 <= item[0] < item[1] <= filesize
            ):
                raise ValueError(f'Invalid completed byte range: {item!r}')
            normalized.append((item[0], item[1]))

        merged = []
        for start, end in sorted(normalized):
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        return merged


    def _write_download_state(self, temp, state):
        state_path = self._download_state_path(temp)
        state_temp = Path(f'{state_path}.tmp')
        with open(state_temp, 'w', encoding='utf-8') as state_file:
            json.dump(state, state_file, indent=2, sort_keys=True)
            state_file.flush()
            os.fsync(state_file.fileno())
        state_temp.replace(state_path)


    def _valid_download_state(self, state, file_id, filesize, validator, temp):
        try:
            if (
                    state.get('version') != 2
                    or state.get('file_id') != file_id
                    or state.get('filesize') != filesize
                    or state.get('validator') != validator
            ):
                return None
            completed = self._normalize_completed(state.get('completed', []), filesize)
            temp_path = Path(temp)
            highest_written = max((end for _, end in completed), default=0)
            if not temp_path.exists() or temp_path.stat().st_size < highest_written:
                return None
            return self._new_download_state(file_id, filesize, validator, completed)
        except (AttributeError, OSError, TypeError, ValueError):
            return None


    def _migrate_legacy_download(self, temp, legacy_state, file_id, filesize, validator):
        try:
            part_count = legacy_state['part_count']
            if (
                    legacy_state.get('version') != 1
                    or legacy_state.get('file_id') != file_id
                    or legacy_state.get('filesize') != filesize
                    or legacy_state.get('validator') != validator
                    or type(part_count) is not int
                    or not 1 <= part_count <= min(filesize, 10000)
            ):
                return None
        except (AttributeError, KeyError, TypeError):
            return None

        temp_path = Path(temp)
        completed = []
        if temp_path.exists() and 0 < temp_path.stat().st_size <= filesize:
            completed.append([0, temp_path.stat().st_size])
            mode = 'r+b'
        else:
            mode = 'w+b'

        ranges = self._download_part_ranges(filesize, part_count)
        with open(temp_path, mode) as output_file:
            for index, (start, end) in enumerate(ranges):
                part_path = Path(f'{temp}.part{index}')
                if not part_path.exists():
                    continue
                part_size = part_path.stat().st_size
                expected_size = end - start + 1
                if part_size > expected_size:
                    continue
                output_file.seek(start)
                with open(part_path, 'rb') as part_file:
                    while True:
                        chunk = part_file.read(self.chunk_copy_size)
                        if not chunk:
                            break
                        output_file.write(chunk)
                if part_size:
                    completed.append([start, start + part_size])
            output_file.flush()
            os.fsync(output_file.fileno())

        state = self._new_download_state(
            file_id,
            filesize,
            validator,
            self._normalize_completed(completed, filesize),
        )
        self._write_download_state(temp, state)
        for path in self._legacy_download_parts(temp):
            path.unlink()
        self._info('Migrated legacy part files into a single temporary file.')
        return state


    def _prepare_download_state(self, temp, file_id, filesize, validator):
        state_path = self._download_state_path(temp)
        state_temp = Path(f'{state_path}.tmp')
        candidates = []
        legacy_state = None
        had_state_file = state_path.exists() or state_temp.exists()
        for path in (state_path, state_temp):
            try:
                raw_state = json.loads(path.read_text(encoding='utf-8'))
            except (OSError, ValueError, TypeError):
                continue
            valid_state = self._valid_download_state(raw_state, file_id, filesize, validator, temp)
            if valid_state:
                candidates.append(valid_state)
            elif path == state_path and isinstance(raw_state, dict) and raw_state.get('version') == 1:
                legacy_state = raw_state

        if candidates:
            state = max(
                candidates,
                key=lambda candidate: sum(end - start for start, end in candidate['completed']),
            )
            self._write_download_state(temp, state)
            return state

        if legacy_state:
            migrated = self._migrate_legacy_download(temp, legacy_state, file_id, filesize, validator)
            if migrated:
                return migrated

        temp_path = Path(temp)
        if temp_path.exists() and temp_path.stat().st_size:
            if had_state_file:
                self._warn('Download state does not match the remote file; restarting safely.')
            else:
                self._warn('Temporary file has no range state; restarting safely.')
        with open(temp_path, 'wb'):
            pass
        self._cleanup_download_state(temp, legacy_parts=True)
        state = self._new_download_state(file_id, filesize, validator, [])
        self._write_download_state(temp, state)
        return state


    def _make_download_session(self):
        """Create an isolated connection pool for one download worker."""
        session = requests_retry_session()
        session.headers.update(self.session.headers)
        session.cookies.update(self.session.cookies)
        session.proxies.update(self.session.proxies)
        session.auth = self.session.auth
        session.verify = self.session.verify
        session.cert = self.session.cert
        session.trust_env = self.session.trust_env
        session.request = functools.partial(
            session.request,
            timeout=(min(10, self.timeout), self.timeout),
        )
        return session


    @staticmethod
    def _missing_download_ranges(completed, filesize):
        missing = []
        cursor = 0
        for start, end in completed:
            if cursor < start:
                missing.append((cursor, start))
            cursor = max(cursor, end)
        if cursor < filesize:
            missing.append((cursor, filesize))
        return missing


    def _download_blocks(self, missing):
        missing_size = sum(end - start for start, end in missing)
        block_size = min(
            DOWNLOAD_BLOCK_SIZE,
            max(1, math.ceil(missing_size / self.download_threads)),
        )
        blocks = []
        for start, end in missing:
            while start < end:
                block_end = min(start + block_size, end)
                blocks.append((start, block_end))
                start = block_end
        return blocks


    def _download_block(
            self, session, download_url, temp, start, end, filesize, validator, progress, task_id, cancel_event):
        range_start = start
        attempts = max(1, self.max_retries)
        for attempt in range(attempts):
            if cancel_event.is_set():
                return session, False
            if session is None:
                session = self._make_download_session()

            headers = {
                'Accept-Encoding': 'identity',
                'Range': f'bytes={range_start}-{end - 1}',
            }
            if validator:
                headers['If-Range'] = validator

            response = None
            try:
                response = session.get(download_url, headers=headers, stream=True)
                response.raise_for_status()
                content_range = parse_content_range(response.headers.get('Content-Range'))
                if response.status_code != 206 or content_range != (range_start, end - 1, filesize):
                    raise RangeResponseError(
                        f'Invalid range response for bytes {range_start}-{end - 1}: '
                        f'HTTP {response.status_code}, Content-Range={response.headers.get("Content-Range")!r}'
                    )
                content_length = response.headers.get('Content-Length')
                if content_length and int(content_length) != end - range_start:
                    raise RangeResponseError(
                        f'Inconsistent Content-Length for bytes {range_start}-{end - 1}: {content_length!r}'
                    )

                with open(temp, 'r+b', buffering=0) as output_file:
                    output_file.seek(range_start)
                    for chunk in response.iter_content(chunk_size=self.chunk_copy_size):
                        if cancel_event.is_set():
                            return session, False
                        if range_start + len(chunk) > end:
                            raise RangeResponseError(f'Response exceeded requested range ending at {end - 1}.')
                        written = output_file.write(chunk)
                        if written != len(chunk):
                            raise OSError(f'Short write: expected {len(chunk)} bytes, wrote {written}.')
                        range_start += written
                        if progress is not None:
                            progress.update(task_id, advance=written)
                    if range_start != end:
                        raise requests.ConnectionError(
                            f'Range ended early at byte {range_start}; expected {end}.'
                        )
                    os.fsync(output_file.fileno())
                return session, True
            except RangeResponseError:
                if response is not None:
                    response.close()
                session.close()
                raise
            except Exception as ex:
                if range_start == end:
                    with open(temp, 'r+b') as output_file:
                        output_file.flush()
                        os.fsync(output_file.fileno())
                    return session, True
                if response is not None:
                    response.close()
                session.close()
                session = None
                if attempt + 1 >= attempts:
                    raise RuntimeError(
                        f'Range {start}-{end - 1} failed after {attempts} attempts.'
                    ) from ex
                wait = min(2 ** attempt, 30)
                self._warn(
                    f'Range {range_start}-{end - 1} failed: {ex} '
                    f'Retrying in {wait}s ({attempt + 1}/{attempts})...'
                )
                if cancel_event.wait(wait):
                    return session, False
            finally:
                if response is not None:
                    response.close()
        return session, False


    def _download_worker(
            self, work_queue, download_url, temp, filesize, validator, state, state_lock,
            progress, task_id, cancel_event):
        session = self._make_download_session()
        try:
            while not cancel_event.is_set():
                try:
                    start, end = work_queue.get_nowait()
                except queue.Empty:
                    return
                try:
                    session, completed = self._download_block(
                        session,
                        download_url,
                        temp,
                        start,
                        end,
                        filesize,
                        validator,
                        progress,
                        task_id,
                        cancel_event,
                    )
                    if not completed:
                        return
                    with state_lock:
                        state['completed'] = self._normalize_completed(
                            [*state['completed'], [start, end]],
                            filesize,
                        )
                        self._write_download_state(temp, state)
                finally:
                    work_queue.task_done()
        finally:
            if session is not None:
                session.close()


    def _download_ranges(self, download_url, filename, temp, file_id, filesize, validator):
        state = self._prepare_download_state(temp, file_id, filesize, validator)
        missing = self._missing_download_ranges(state['completed'], filesize)
        blocks = self._download_blocks(missing)
        completed_size = sum(end - start for start, end in state['completed'])
        connections = min(self.download_threads, len(blocks))

        if completed_size:
            active_connections = max(1, connections)
            self._info(
                f'Resuming at {bytes_to_size_str(completed_size)} '
                f'({completed_size / filesize:.1%}) with {active_connections} '
                f'{"connection" if active_connections == 1 else "connections"}.'
            )
        elif connections:
            self._info(f'Downloading with {connections} connections.')

        progress = None
        task_id = None
        if self.progress:
            desc = filename if len(filename) <= 20 else filename[0:11] + '..' + filename[-7:]
            progress = make_progress(self.console)
            task_id = progress.add_task(desc, total=filesize, completed=completed_size)
            progress.start()

        work_queue = queue.Queue()
        for block in blocks:
            work_queue.put(block)
        state_lock = threading.Lock()
        cancel_event = threading.Event()
        try:
            if connections:
                with concurrent.futures.ThreadPoolExecutor(max_workers=connections) as executor:
                    futures = [
                        executor.submit(
                            self._download_worker,
                            work_queue,
                            download_url,
                            temp,
                            filesize,
                            validator,
                            state,
                            state_lock,
                            progress,
                            task_id,
                            cancel_event,
                        )
                        for _ in range(connections)
                    ]
                    try:
                        for future in concurrent.futures.as_completed(futures):
                            future.result()
                    except BaseException:
                        cancel_event.set()
                        for future in futures:
                            future.cancel()
                        raise
        finally:
            if progress is not None:
                progress.stop()

        completed = self._normalize_completed(state['completed'], filesize)
        if completed != [[0, filesize]]:
            raise RuntimeError('Download stopped before all byte ranges completed.')
        with open(temp, 'r+b') as output_file:
            output_file.truncate(filesize)
            output_file.flush()
            os.fsync(output_file.fileno())
        self._cleanup_download_state(temp, legacy_parts=True)


    def _probe_download_response(self, download_url):
        """Fetch metadata and range support together; retain a 200 body for fallback."""
        response = self.session.get(
            download_url,
            headers={'Accept-Encoding': 'identity', 'Range': 'bytes=0-0'},
            stream=True,
        )
        try:
            # An empty file has no satisfiable byte range. Fetch its empty body
            # normally so filename and validator headers remain authoritative.
            if response.status_code == 416 and response.headers.get('Content-Range') == 'bytes */0':
                response.close()
                response = self.session.get(
                    download_url, headers={'Accept-Encoding': 'identity'}, stream=True,
                )
            response.raise_for_status()
            if response.status_code == 200:
                filesize = int(response.headers['Content-Length'])
                if filesize < 0:
                    raise RuntimeError('Invalid Content-Length for download.')
                if filesize:
                    self._warn(
                        'Server does not support byte ranges; using a non-resumable single connection.'
                    )
                return response, filesize
            if response.status_code != 206:
                raise RuntimeError(
                    f'Unexpected HTTP {response.status_code} response to range probe.'
                )

            content_range = parse_content_range(response.headers.get('Content-Range'))
            if not content_range or content_range[:2] != (0, 0) or content_range[2] <= 0:
                raise RuntimeError(
                    f'Invalid Content-Range for ranged download: '
                    f'{response.headers.get("Content-Range")!r}'
                )
            if response.headers.get('Content-Length', '1') != '1':
                raise RuntimeError('Invalid Content-Length for range probe.')
            # Drain the one-byte response to allow connection reuse.
            received = 0
            for chunk in response.iter_content(chunk_size=2):
                received += len(chunk)
                if received > 1:
                    raise RuntimeError('Range probe exceeded one byte.')
            if received != 1:
                raise RuntimeError('Range probe ended before the expected byte.')
            return response, content_range[2]
        except BaseException:
            response.close()
            raise


    def _download_sequentially(self, response, filename, temp, filesize):
        progress = None
        task_id = None
        if self.progress:
            desc = filename if len(filename) <= 20 else filename[0:11] + '..' + filename[-7:]
            progress = make_progress(self.console)
            task_id = progress.add_task(desc, total=filesize)
            progress.start()

        try:
            self._cleanup_download_state(temp, legacy_parts=True)
            with open(temp, 'wb') as output_file:
                for chunk in response.iter_content(chunk_size=self.chunk_copy_size):
                    output_file.write(chunk)
                    if progress is not None:
                        progress.update(task_id, advance=len(chunk))
        finally:
            if progress is not None:
                progress.stop()


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

            response, filesize = self._probe_download_response(download_url)
            try:
                header_name = filename_from_content_disposition(response.headers.get('Content-Disposition'))
                filename = self._resolve_filename(header_name, web_name, output, idx, total)
                temp = filename + '.dl'
                validator = response.headers.get('ETag') or response.headers.get('Last-Modified')
                self._info(f'Name: {filename}, size: {size_str}, id: {file_id}')

                if response.status_code == 206:
                    response.close()
                    response = None
                    self._download_ranges(
                        download_url,
                        filename,
                        temp,
                        file_id,
                        filesize,
                        validator,
                    )

                if response is not None:
                    self._download_sequentially(response, filename, temp, filesize)
            finally:
                if response is not None:
                    response.close()

            filesize_downloaded = Path(temp).stat().st_size
            if filesize == filesize_downloaded:
                self.console.print(f'[green]Filesize check passed ({bytes_to_size_str(filesize)}). Succeeded.')
                rename(temp, filename)
                self._cleanup_download_state(temp, legacy_parts=True)
            else:
                self._err(f'ERROR: Downloaded file is corrupt (expected {filesize}, got {filesize_downloaded}). '
                          f'Please check the broken file at {temp} and delete it yourself if needed.')
            downloaded.append(filename)
        return downloaded
