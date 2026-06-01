# gfile

A python CLI/module to download and upload from [gigafile](https://gigafile.nu/).

This is a fork of [fireattack/gfile](https://github.com/fireattack/gfile) (itself a major update from [the original](https://github.com/Sraq-Zit/gfile)), with additional changes:

* Stable, prettier progress bars via `rich`; error/retry logs no longer corrupt the bars
* More robust uploads over flaky/proxied networks: smaller default chunks, longer timeout, exponential-backoff retries (`--max-retries`, `--proxy`)
* Reliable download filenames sourced from the `Content-Disposition` header (works even before the page renders / when the page name is masked)
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

$ gfile -h
usage: Gfile [-h] [--version] [-p] [-o OUTPUT] [--aria2 [ARIA2]] [-n THREAD_NUM] [-s CHUNK_SIZE] [-m CHUNK_COPY_SIZE] [-t TIMEOUT]
             [-r MAX_RETRIES] [--proxy PROXY] [-k KEY] [--mute] [--verify | --no-verify]
             {download,upload} file_or_url

positional arguments:
  {download,upload}     upload or download
  file_or_url           filename to upload or url to download

options:
  -h, --help            show this help message and exit
  --version             show version and exit
  -p, --hide-progress   hide progress bar
  -o OUTPUT, --output OUTPUT
                        output filename for download (default: use original name)
  --aria2 [ARIA2]       download with aria2. You can also specify optional arguments (default: "-x10 -s10", make sure to quote). `-o` is already
                        automatically included.
  -n THREAD_NUM, --thread-num THREAD_NUM
                        number of threads used for upload [default: 8]
  -s CHUNK_SIZE, --chunk-size CHUNK_SIZE
                        chunk size per upload in bytes; note: ~2*chunk_size*thread may be loaded into memory. Smaller chunks waste less on retries
                        over flaky networks [default: 30MB]
  -m CHUNK_COPY_SIZE, --copy-size CHUNK_COPY_SIZE
                        specifies size to copy the main file into pieces [default: 1MB]
  -t TIMEOUT, --timeout TIMEOUT
                        read timeout (in seconds); connect timeout is min(10, timeout) [default: 30]
  -r MAX_RETRIES, --max-retries MAX_RETRIES
                        max retry attempts per chunk, with exponential backoff [default: 10]
  --proxy PROXY         proxy URL for upload/download, e.g. http://127.0.0.1:7890 (HTTPS_PROXY/HTTP_PROXY env vars are also honored)
  -k KEY, --key KEY, --password KEY
                        specifies the key/password for the file
  --mute                mute initial message and warnings (only the final result and errors will be shown)
  --verify              enable verification (default)
  --no-verify           disable verification
```

### Module
#### Import
```py
from gfile import GFile
```
#### Download
```py
filename = GFile('https://XX.gigafile.nu/YYY').download()
```

#### Upload
```py
url = GFile('path/to/file', progress=True).upload().get_download_page()
```