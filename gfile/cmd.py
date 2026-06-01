
import argparse
from enum import Enum

if __name__ == '__main__' and __package__ is None:
    from gfile import GFile
    from __init__ import __version__
else:
    from .gfile import GFile
    from . import __version__

class Action(Enum):
    download = 'download'
    upload = 'upload'
    def __str__(self):
        return self.value

def main():
    parser = argparse.ArgumentParser(prog='Gfile')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}', help='show version and exit')
    parser.add_argument('action', type=Action, choices=list(Action), help='upload or download')
    parser.add_argument('file_or_url', help='filename to upload or url to download')
    parser.add_argument('-p', '--hide-progress', dest='progress', action='store_false', default=True, help='hide progress bar')
    parser.add_argument('-o', '--output', type=str, default=None, help='output filename for download (default: use original name)')
    parser.add_argument('--aria2', nargs='?', const="-x10 -s10", default=None, help='download with aria2. You can also specify optional arguments (default: "-x10 -s10", make sure to quote). `-o` is already automatically included.')
    parser.add_argument('-n', '--thread-num', dest='thread_num', default=8, type=int, help='number of threads used for upload [default: 8]')
    parser.add_argument('-s', '--chunk-size', dest='chunk_size', default="30MB", help='chunk size per upload in bytes; note: ~2*chunk_size*thread may be loaded into memory. Smaller chunks waste less on retries over flaky networks [default: 30MB]')
    parser.add_argument('-m', '--copy-size', dest='chunk_copy_size', default="1MB", help='specifies size to copy the main file into pieces [default: 1MB]')
    parser.add_argument('-t', '--timeout', type=int, default=30, help='read timeout (in seconds); connect timeout is min(10, timeout) [default: 30]')
    parser.add_argument('-r', '--max-retries', dest='max_retries', type=int, default=10, help='max retry attempts per chunk, with exponential backoff [default: 10]')
    parser.add_argument('--proxy', default=None, help='proxy URL for upload/download, e.g. http://127.0.0.1:7890 (HTTPS_PROXY/HTTP_PROXY env vars are also honored)')
    parser.add_argument('-k', '--key', '--password', dest='key', default=None, help='specifies the key/password for the file')
    parser.add_argument('--mute', action='store_true', help='mute initial message and warnings (only the final result and errors will be shown)')
    verify_group = parser.add_mutually_exclusive_group()
    verify_group.add_argument('--verify', dest='verify', action='store_true', default=True, help='enable verification (default)')
    verify_group.add_argument('--no-verify', dest='verify', action='store_false', help='disable verification')

    args = parser.parse_args()

    gf = GFile(**args.__dict__)
    if args.action == Action.download:
        gf.download(args.output)
    else:
        gf.upload().get_download_page()

if __name__ == "__main__":
    main()
