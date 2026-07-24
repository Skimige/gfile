# gfile

A python CLI/module to download and upload from [gigafile](https://gigafile.nu/).

This is a fork of [fireattack/gfile](https://github.com/fireattack/gfile) (itself a major update from [the original](https://github.com/Sraq-Zit/gfile)), with additional changes:

* Stable, prettier progress bars via `rich`, with consistent IEC size units; error/retry logs no longer corrupt the bars
* Ordered, windowed uploads keep the link busy without risking out-of-order commits, with bounded memory use and clean Ctrl+C cancellation (`--window`, `--thread-num`)
* More robust transfers over flaky/proxied networks: smaller default upload chunks, longer timeout, exponential-backoff retries (`--max-retries`, `--proxy`)
* Reliable download filenames sourced from the `Content-Disposition` header (works even before the page renders / when the page name is masked)
* Resumable downloads with four independent connections by default (`--download-threads`), or via aria2 (`--aria2`)
* Project management migrated to `uv` / `hatchling`

Upstream highlights:

* Fixed multi-thread uploading (and made sure each threads finish in order so the final file is not broken)
* Fixed download filename issue
* Some refactoring and QoL changes.

## Install
With [uv](https://docs.astral.sh/uv/) (recommended):

    $ uv tool install git+https://github.com/Skimige/gfile.git

For local development, run it straight from a clone without installing:

    $ uv run gfile -h

Or with pip:

    $ pip install -U git+https://github.com/Skimige/gfile.git

## Usage
### CLI
```bash
$ gfile upload path/to/file

$ gfile download https://66.gigafile.nu/0320-b36ec21d4a56b143537e12df7388a5367

$ gfile download https://66.gigafile.nu/0320-b36ec21d4a56b143537e12df7388a5367 --download-threads 8

$ gfile -h
usage: Gfile [-h] [--version] [-p] [-o OUTPUT] [--aria2 [ARIA2]]
             [-d DOWNLOAD_THREADS] [-n THREAD_NUM] [-w WINDOW] [-s CHUNK_SIZE]
             [-m CHUNK_COPY_SIZE] [-t TIMEOUT] [-r MAX_RETRIES]
             [--proxy PROXY] [-k KEY] [--mute] [--verify | --no-verify]
             {download,upload} file_or_url

positional arguments:
  {download,upload}     upload or download
  file_or_url           filename to upload or url to download

options:
  -h, --help            show this help message and exit
  --version             show version and exit
  -p, --hide-progress   hide progress bar
  -o, --output OUTPUT   output filename for download (default: use original
                        name)
  --aria2 [ARIA2]       download with aria2. You can also specify optional
                        arguments (default: "-c -x10 -s10", make sure to
                        quote). `-o` is already automatically included.
  -d, --download-threads DOWNLOAD_THREADS
                        number of parallel connections for the built-in
                        downloader; use 1 for sequential download [default: 4]
  -n, --thread-num THREAD_NUM
                        number of worker threads; also the hard cap on in-
                        flight chunks (and thus memory use) when the soft
                        window is exceeded to keep the link busy [default: 8]
  -w, --window WINDOW   target number of chunks uploading concurrently (soft
                        in-flight window; when all in-flight chunks are parked
                        waiting to commit, one extra is admitted so the link
                        never idles). gigafile commits chunks strictly in
                        order, so a smaller window is more robust on
                        slow/flaky links (less wasted re-sending) while a
                        larger one is faster on good ones [default: 2]
  -s, --chunk-size CHUNK_SIZE
                        chunk size per upload in bytes; note: each in-flight
                        chunk holds ~chunk_size in memory, normally ~window+1
                        chunks, up to thread_num when commits are slow.
                        Smaller chunks waste less on retries over flaky
                        networks [default: 30MB]
  -m, --copy-size CHUNK_COPY_SIZE
                        specifies size to copy the main file into pieces
                        [default: 1MB]
  -t, --timeout TIMEOUT
                        read timeout (in seconds); connect timeout is min(10,
                        timeout) [default: 30]
  -r, --max-retries MAX_RETRIES
                        max retry attempts per upload chunk or download range,
                        with exponential backoff [default: 10]
  --proxy PROXY         proxy URL for upload/download, e.g.
                        http://127.0.0.1:7890 (HTTPS_PROXY/HTTP_PROXY env vars
                        are also honored)
  -k, --key, --password KEY
                        specifies the key/password for the file
  --mute                mute initial message and warnings (only the final
                        result and errors will be shown)
  --verify              enable verification (default)
  --no-verify           disable verification
```

The built-in downloader uses one independent HTTP session/TCP connection per worker. Downloaded ranges are written directly into one `.dl` file, while a small `.dl.json` sidecar records completed byte ranges. Keep both files after an interruption; the next run can resume with any `--download-threads` value. A dropped connection is recreated and resumes within its current range using exponential backoff. Use `--download-threads 1` for sequential downloading with the same retry behavior. If the server does not support byte ranges, gfile falls back to a non-resumable single connection.

Uploads commit chunks strictly in order. `--window` controls the soft number of chunks in flight; one extra chunk may be admitted when every active upload is waiting to commit so the link does not idle. Memory use is normally about `window + 1` chunks and is capped by `--thread-num`.

### Module
#### Import
```py
from gfile import GFile
```
#### Download
```py
filename = GFile(
    'https://XX.gigafile.nu/YYY',
    download_threads=8,
).download()
```

#### Upload
```py
url = GFile(
    'path/to/file',
    progress=True,
    thread_num=8,
    window=2,
).upload().get_download_page()
```
