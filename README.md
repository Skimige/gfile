# gfile

A python CLI/module to download and upload from [gigafile](https://gigafile.jp/). Both `.jp` and legacy `.nu` share URLs are accepted.

This is a fork of [fireattack/gfile](https://github.com/fireattack/gfile) (itself a major update from [the original](https://github.com/Sraq-Zit/gfile)), with additional changes:

* Stable, prettier progress bars via `rich`, with consistent IEC size units; error/retry logs no longer corrupt the bars
* Ordered, windowed uploads stream from disk with small buffers, bounded task scheduling and clean Ctrl+C cancellation (`--window`, `--thread-num`)
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

$ gfile download https://66.gigafile.jp/0320-b36ec21d4a56b143537e12df7388a5367

$ gfile download https://66.gigafile.jp/0320-b36ec21d4a56b143537e12df7388a5367 --download-threads 8

$ gfile -h
usage: Gfile [-h] [--version] [-p] [-o OUTPUT] [--aria2 [ARIA2]]
             [-d DOWNLOAD_THREADS] [-n THREAD_NUM] [-w WINDOW] [-s CHUNK_SIZE]
             [-m CHUNK_COPY_SIZE] [-t TIMEOUT] [-r MAX_RETRIES]
             [--proxy PROXY] [-k KEY] [--mute] [--performance-log PATH]
             [--verify | --no-verify]
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
                        number of upload worker threads; also caps pending
                        tasks, open files and in-flight chunks [default: 8]
  -w, --window WINDOW   target number of chunks actively uploading. A chunk
                        waiting to commit gives up its sender slot so the next
                        chunk can start. gigafile commits chunks strictly in
                        order, so a smaller window is more robust on
                        slow/flaky links (less wasted re-sending) while a
                        larger one is faster on good ones [default: 2]
  -s, --chunk-size CHUNK_SIZE
                        chunk size per upload in bytes; streamed from disk
                        without buffering the whole chunk. Smaller chunks
                        waste less on retries over flaky networks
                        [default: 30MB]
  -m, --copy-size CHUNK_COPY_SIZE
                        maximum file read size for uploads and buffer size
                        for downloads [default: 1MB]
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
  --performance-log PATH
                        append upload timing events and 5-second snapshots
                        to a JSONL file (upload only)
  --verify              enable verification (default)
  --no-verify           disable verification
```

The built-in downloader uses one independent HTTP session/TCP connection per worker. Downloaded ranges are written directly into one `.dl` file, while a small `.dl.json` sidecar records completed byte ranges. Keep both files after an interruption; the next run can resume with any `--download-threads` value. A dropped connection is recreated and resumes within its current range using exponential backoff. Use `--download-threads 1` for sequential downloading with the same retry behavior. If the server does not support byte ranges, gfile falls back to a non-resumable single connection.

Uploads commit chunks strictly in order. `--window` controls the target number of chunks actively sending data. A chunk waiting to commit releases its sender slot immediately, allowing the next chunk to start without waiting for every active upload to catch up. Each worker streams its file range through the multipart encoder in 128 KiB pieces instead of allocating the whole chunk. Pending tasks, open files and in-flight chunks are capped by `--thread-num`. Retries rewind the file range and rebuild the request, so do not modify the source file during an upload.

### Transfer Performance

* Upload memory no longer scales with `--chunk-size`. Larger chunks reduce HTTP request overhead, but still require more bytes to be resent after a failure. The connection pool retains enough connections for the configured upload worker count.
* Downloads fetch size, filename, validator and Range support in a single one-byte request. If the server returns a full body instead, that same response is used for sequential downloading. This avoids the previous extra full-file GET that was closed after reading only its headers.
* Download workers already reuse independent connections and write directly into one temporary file, without a final part-file merge. Completed blocks are still synced before their resume records are saved; this durability guarantee has not been weakened for speed.
* Keep the default upload window of 2 and download connection count of 4 as a baseline. Compare elapsed time and retry counts while changing only one setting at a time. More connections can help a per-connection bottleneck, but cannot increase a saturated network or disk's capacity. Upload worker count must be at least the window size to make that window effective.

Further changes should follow measurements on the intended connection: larger or adaptive download blocks could reduce request and sync overhead, but increase unfinished work after interruption and leave slow workers holding larger ranges. Batching resume checkpoints would reduce sync frequency, but also needs explicit crash-recovery tests. Local buffer measurements alone do not establish a speedup against the remote service.

Run the transfer regression tests with `uv run python -m unittest discover -s tests -v`.

### Upload Performance Logs

```powershell
uv run gfile upload "path\to\file.bin" --performance-log upload-perf.jsonl
Get-Content .\upload-perf.jsonl -Tail 20 -Wait
```

Logging is opt-in and works with `--mute` and `--hide-progress`. JSONL records are appended, with a separate `run_id` for each upload. A background thread writes and flushes events and a snapshot every 5 seconds, including while all workers are waiting. An invalid log path fails before the upload starts; a later log write failure disables logging without aborting the transfer. The log path must not be the source file.

The log contains upload settings, the selected server hostname, phase transitions, attempt results, retries, committed bytes and a final outcome. Chunk and attempt numbers start at 1, matching the progress display. It excludes source filenames, share URLs, tokens, passwords, cookies, proxy addresses and request/response bodies. Exceptions are recorded as class names, including nested causes, without their potentially sensitive messages. Hostnames, file sizes and timings are still visible in the log.

* `waiting_admission`: waiting for an upload sender slot.
* `preparing`: opening the source and preparing multipart fields.
* `sending`: preparing the attempt, connecting and supplying the bulk body to Requests.
* `waiting_turn`: bulk body supplied; waiting for earlier chunks to commit.
* `sending_tail`: supplying the held-back final body segment.
* `waiting_response`: body generator exhausted; waiting for the POST response and decoding its JSON.
* `backoff`: waiting before retrying the chunk.

`upload_attempt.phase_seconds` separates the sending, ordered waiting and response waiting time for each attempt. `upload_snapshot.active_chunks` includes the current phase and its age, so a stalled attempt is visible before it finishes. Snapshots include interval byte rates as well as the cumulative committed-byte rate. `body_bytes_yielded` includes multipart overhead and retransmissions and measures bytes supplied to Requests, not bytes confirmed on the wire. `committed_bytes` counts raw file bytes only after a successful server response. Aggregate `attempt_phase_seconds` sums finished attempts across workers, so overlapping waits can exceed wall-clock time.

These are application-level timings, not separate DNS, TLS, socket-write or server-processing measurements. A long `waiting_turn` points to an earlier chunk; a long `waiting_response` needs investigation of the server, network or proxy. The `upload_end` outcome covers uploading only, before the optional remote filesize verification. A forced process termination may leave no final outcome; completed log lines remain usable.

### Module
#### Import
```py
from gfile import GFile
```
#### Download
```py
filename = GFile(
    'https://XX.gigafile.jp/YYY',
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
