import json
import re
import tempfile
import threading
import unittest
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from gfile.gfile import GFile


class TransferHTTPTests(unittest.TestCase):
    def test_streaming_upload_and_ranged_download_round_trip(self):
        chunks = []
        download_requests = []
        errors = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def reply(self, status, body, headers=None):
                self.send_response(status)
                self.send_header('Content-Length', str(len(body)))
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if not self.path.startswith('/download.php'):
                    self.reply(200, (
                        b'var server = "1.gigafile.jp";'
                        b'<span class="dl_size">1MiB</span><span id="dl">masked.bin</span>'
                    ), {'Set-Cookie': 'transfer=test; Path=/'})
                    return
                if self.headers.get('Cookie') != 'transfer=test':
                    errors.append('missing download cookie')
                body = b''.join(chunks)
                requested = self.headers.get('Range')
                download_requests.append(requested)
                match = re.fullmatch(r'bytes=(\d+)-(\d+)', requested or '')
                if not match:
                    errors.append('unexpected full-file GET')
                    self.reply(400, b'')
                    return
                start, end = map(int, match.groups())
                self.reply(206, body[start:end + 1], {
                    'Content-Range': f'bytes {start}-{end}/{len(body)}',
                    'Content-Disposition': 'attachment; filename="example.bin"',
                    'ETag': '"round-trip"',
                })

            def do_POST(self):
                if self.headers.get('Transfer-Encoding'):
                    errors.append('unexpected chunked transfer encoding')
                if self.headers.get('Cookie') != 'transfer=test':
                    errors.append('missing upload cookie')
                body = self.rfile.read(int(self.headers['Content-Length']))
                message = BytesParser().parsebytes(
                    f'Content-Type: {self.headers["Content-Type"]}\r\n\r\n'.encode() + body
                )
                fields = {
                    part.get_param('name', header='content-disposition'): part.get_payload(decode=True)
                    for part in message.get_payload()
                }
                if int(fields['chunk']) != len(chunks):
                    errors.append('out-of-order upload commit')
                chunks.append(fields['file'])
                result = {'status': 0}
                if len(chunks) == int(fields['chunks']):
                    result['url'] = 'https://1.gigafile.jp/roundtrip'
                self.reply(200, json.dumps(result).encode())

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                source = Path(directory) / 'source.bin'
                target = Path(directory) / 'target.bin'
                content = bytes(range(256)) * 4096
                source.write_bytes(content)
                log_path = Path(directory) / 'performance.jsonl'
                uploader = GFile(source, chunk_size=300000, mute=True, max_retries=1, performance_log=log_path)
                downloader = GFile('https://1.gigafile.jp/roundtrip', mute=True, max_retries=1)
                try:
                    for gfile in (uploader, downloader):
                        gfile.session.trust_env = False
                    original_upload_request = uploader.session.request
                    original_download_request = downloader.session.request

                    def local_url(url):
                        return urlsplit(url)._replace(
                            scheme='http', netloc=f'127.0.0.1:{server.server_port}',
                        ).geturl()

                    def upload_request(method, url, **kwargs):
                        return original_upload_request(method, local_url(url), **kwargs)

                    def download_request(method, url, **kwargs):
                        return original_download_request(method, local_url(url), **kwargs)

                    original_make_session = downloader._make_download_session

                    def make_session():
                        session = original_make_session()
                        request = session.request
                        session.request = lambda method, url, **kw: request(method, local_url(url), **kw)
                        return session

                    with patch.object(uploader.session, 'request', side_effect=upload_request):
                        uploader.upload()
                    self.assertFalse(uploader.failed)
                    self.assertEqual(b''.join(chunks), content)
                    log = [json.loads(line) for line in log_path.read_text(encoding='utf-8').splitlines()]
                    self.assertEqual(log[-1]['outcome'], 'succeeded')
                    self.assertEqual(log[-1]['committed_bytes'], len(content))
                    self.assertEqual([item['chunk'] for item in log if item['event'] == 'upload_commit'], [1, 2, 3, 4])
                    attempts = [item for item in log if item['event'] == 'upload_attempt']
                    self.assertEqual(len(attempts), 4)
                    self.assertTrue(all(item['body_bytes'] == item['body_bytes_yielded'] for item in attempts))
                    with patch.object(downloader.session, 'request', side_effect=download_request), patch.object(
                        downloader, '_make_download_session', side_effect=make_session,
                    ):
                        self.assertEqual(downloader.download(str(target)), [str(target)])
                    self.assertEqual(target.read_bytes(), content)
                    self.assertEqual(download_requests[0], 'bytes=0-0')
                    self.assertEqual(len(download_requests), 5)
                    self.assertEqual(errors, [])
                finally:
                    uploader.session.close()
                    downloader.session.close()
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()


if __name__ == '__main__':
    unittest.main()
