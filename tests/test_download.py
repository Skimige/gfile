import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from gfile import cmd
from gfile.gfile import GFile, normalize_gigafile_url, parse_content_range


class FakeResponse:
    def __init__(self, body, status_code=200, headers=None, stream_error=None):
        self.body = body
        self.status_code = status_code
        self.headers = headers or {}
        self.stream_error = stream_error
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset:offset + chunk_size]
        if self.stream_error:
            raise self.stream_error

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return next(self.responses)


class RangeSession:
    def __init__(self, responses):
        self.responses = responses
        self.requests = []
        self.closed = False

    def get(self, url, **kwargs):
        request_range = kwargs['headers']['Range']
        self.requests.append((url, kwargs))
        return self.responses[request_range]

    def close(self):
        self.closed = True


class RangeSessionFactory:
    def __init__(self, responses):
        self.responses = responses
        self.sessions = []

    def __call__(self):
        session = RangeSession(self.responses)
        self.sessions.append(session)
        return session


def response(body, status_code=200, content_range=None, content_length=None, stream_error=None):
    headers = {
        'Content-Length': str(len(body) if content_length is None else content_length),
        'Content-Disposition': 'attachment; filename="example.bin"',
        'ETag': '"test-etag"',
    }
    if content_range:
        headers['Content-Range'] = content_range
    return FakeResponse(body, status_code=status_code, headers=headers, stream_error=stream_error)


class DownloadTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.previous_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)

    def tearDown(self):
        os.chdir(self.previous_cwd)
        self.temp_dir.cleanup()

    def make_gfile(self, responses, download_threads=4, size='10B'):
        gfile = GFile(
            'https://1.gigafile.nu/share',
            progress=False,
            mute=True,
            download_threads=download_threads,
        )
        gfile.parse_download_page = lambda _: [('example.bin', size, 'share')]
        gfile.session = FakeSession(responses)
        return gfile


class DownloadResumeTests(DownloadTestCase):
    def test_resumes_existing_temporary_file(self):
        Path('example.bin.dl').write_bytes(b'abcd')
        probe = response(b'a', status_code=206, content_range='bytes 0-0/10')
        gfile = self.make_gfile([probe], download_threads=1)
        gfile._write_download_state('example.bin.dl', {
            'version': 2,
            'file_id': 'share',
            'filesize': 10,
            'validator': '"test-etag"',
            'completed': [[0, 4]],
        })
        session_factory = RangeSessionFactory({
            'bytes=4-9': response(b'efghij', status_code=206, content_range='bytes 4-9/10'),
        })
        gfile._make_download_session = session_factory

        self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), b'abcdefghij')
        self.assertFalse(Path('example.bin.dl').exists())
        self.assertTrue(probe.closed)
        self.assertEqual(len(gfile.session.requests), 1)
        self.assertEqual(gfile.session.requests[0][1]['headers']['Range'], 'bytes=0-0')
        self.assertEqual(session_factory.sessions[0].requests[0][1]['headers']['Range'], 'bytes=4-9')
        self.assertEqual(session_factory.sessions[0].requests[0][1]['headers']['If-Range'], '"test-etag"')

    def test_restarts_when_server_ignores_range(self):
        Path('example.bin.dl').write_bytes(b'abcd')
        restarted = response(b'abcdefghij')
        gfile = self.make_gfile([restarted])

        self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), b'abcdefghij')
        self.assertEqual(len(gfile.session.requests), 1)
        self.assertTrue(restarted.closed)

    def test_finishes_complete_temporary_file_without_range_workers(self):
        Path('example.bin.dl').write_bytes(b'abcdefghij')
        probe = response(b'a', status_code=206, content_range='bytes 0-0/10')
        gfile = self.make_gfile([probe])
        gfile._write_download_state('example.bin.dl', {
            'version': 2,
            'file_id': 'share',
            'filesize': 10,
            'validator': '"test-etag"',
            'completed': [[0, 10]],
        })
        session_factory = RangeSessionFactory({})
        gfile._make_download_session = session_factory

        self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), b'abcdefghij')
        self.assertEqual(len(gfile.session.requests), 1)
        self.assertEqual(session_factory.sessions, [])

    def test_does_not_trust_temporary_file_without_range_state(self):
        Path('example.bin.dl').write_bytes(b'xxxx')
        probe = response(b'a', status_code=206, content_range='bytes 0-0/10')
        gfile = self.make_gfile([probe], download_threads=1)
        session_factory = RangeSessionFactory({
            'bytes=0-9': response(b'abcdefghij', status_code=206, content_range='bytes 0-9/10'),
        })
        gfile._make_download_session = session_factory

        self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), b'abcdefghij')
        self.assertEqual(session_factory.sessions[0].requests[0][1]['headers']['Range'], 'bytes=0-9')

    def test_rejects_inconsistent_content_range(self):
        Path('example.bin.dl').write_bytes(b'abcd')
        probe = response(b'a', status_code=206, content_range='bytes 5-5/10')
        gfile = self.make_gfile([probe])

        with self.assertRaisesRegex(RuntimeError, 'Invalid Content-Range'):
            gfile.download()

        self.assertEqual(Path('example.bin.dl').read_bytes(), b'abcd')


class DownloadProbeTests(DownloadTestCase):
    def test_downloads_empty_file_after_unsatisfiable_probe(self):
        probe = response(b'', status_code=416, content_range='bytes */0')
        empty = response(b'')
        gfile = self.make_gfile([probe, empty], size='0B')
        self.assertEqual(gfile.download(), ['example.bin'])
        self.assertEqual(Path('example.bin').read_bytes(), b'')
        self.assertTrue(probe.closed)
        self.assertTrue(empty.closed)
        self.assertNotIn('Range', gfile.session.requests[1][1]['headers'])

    def test_rejects_invalid_probe_without_creating_output(self):
        for probe in (
            response(b'', status_code=206, content_range='bytes 0-0/10', content_length=1),
            response(b'ab', status_code=206, content_range='bytes 0-0/10', content_length=1),
            response(b'a', status_code=206, content_range='bytes 0-0/10', content_length=2),
            response(b'a', status_code=206, content_range='bytes 0-0/0'),
            response(b'', status_code=416, content_range='bytes */10'),
        ):
            with self.subTest(headers=probe.headers, body=probe.body):
                gfile = self.make_gfile([probe])
                with self.assertRaises(RuntimeError):
                    gfile.download()
                self.assertTrue(probe.closed)
                self.assertFalse(Path('example.bin.dl').exists())


class ParallelDownloadTests(DownloadTestCase):
    def test_downloads_four_ranges_with_independent_sessions(self):
        body = b'abcdefghijkl'
        probe = response(b'a', status_code=206, content_range='bytes 0-0/12')
        gfile = self.make_gfile([probe], size='12B')
        session_factory = RangeSessionFactory({
            'bytes=0-2': response(b'abc', status_code=206, content_range='bytes 0-2/12'),
            'bytes=3-5': response(b'def', status_code=206, content_range='bytes 3-5/12'),
            'bytes=6-8': response(b'ghi', status_code=206, content_range='bytes 6-8/12'),
            'bytes=9-11': response(b'jkl', status_code=206, content_range='bytes 9-11/12'),
        })
        gfile._make_download_session = session_factory

        self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), body)
        self.assertEqual(len(session_factory.sessions), 4)
        self.assertTrue(all(session.closed for session in session_factory.sessions))
        requested_ranges = {
            session.requests[0][1]['headers']['Range']
            for session in session_factory.sessions
        }
        self.assertEqual(requested_ranges, {'bytes=0-2', 'bytes=3-5', 'bytes=6-8', 'bytes=9-11'})
        self.assertFalse(Path('example.bin.dl.json').exists())
        self.assertFalse(any(Path('.').glob('example.bin.dl.part*')))

    def test_resumes_state_with_a_different_thread_count(self):
        body = b'abcdefghijkl'
        probe = response(b'a', status_code=206, content_range='bytes 0-0/12')
        gfile = self.make_gfile([probe], download_threads=2, size='12B')
        partial = bytearray(12)
        partial[0:3] = b'abc'
        partial[6:9] = b'ghi'
        Path('example.bin.dl').write_bytes(partial)
        gfile._write_download_state('example.bin.dl', {
            'version': 2,
            'file_id': 'share',
            'filesize': 12,
            'validator': '"test-etag"',
            'completed': [[0, 3], [6, 9]],
        })
        session_factory = RangeSessionFactory({
            'bytes=3-5': response(b'def', status_code=206, content_range='bytes 3-5/12'),
            'bytes=9-11': response(b'jkl', status_code=206, content_range='bytes 9-11/12'),
        })
        gfile._make_download_session = session_factory

        self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), body)
        requested_ranges = {
            session.requests[0][1]['headers']['Range']
            for session in session_factory.sessions
        }
        self.assertEqual(requested_ranges, {'bytes=3-5', 'bytes=9-11'})
        self.assertEqual(len(session_factory.sessions), 2)

    def test_migrates_legacy_part_files_into_single_data_file(self):
        body = b'abcdefghijkl'
        probe = response(b'a', status_code=206, content_range='bytes 0-0/12')
        gfile = self.make_gfile([probe], download_threads=2, size='12B')
        Path('example.bin.dl.json').write_text(json.dumps({
            'version': 1,
            'file_id': 'share',
            'filesize': 12,
            'validator': '"test-etag"',
            'part_count': 4,
        }), encoding='utf-8')
        Path('example.bin.dl.part0').write_bytes(b'abc')
        Path('example.bin.dl.part1').write_bytes(b'd')
        Path('example.bin.dl.part3').write_bytes(b'jkl')
        session_factory = RangeSessionFactory({
            'bytes=4-6': response(b'efg', status_code=206, content_range='bytes 4-6/12'),
            'bytes=7-8': response(b'hi', status_code=206, content_range='bytes 7-8/12'),
        })
        gfile._make_download_session = session_factory

        self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), body)
        self.assertFalse(any(Path('.').glob('example.bin.dl.part*')))

    def test_reconnects_and_resumes_a_block_after_stream_failure(self):
        body = b'abcdefghijkl'
        probe = response(b'a', status_code=206, content_range='bytes 0-0/12')
        gfile = self.make_gfile([probe], download_threads=2, size='12B')
        session_factory = RangeSessionFactory({
            'bytes=0-5': response(
                b'abc',
                status_code=206,
                content_range='bytes 0-5/12',
                content_length=6,
                stream_error=ConnectionError('connection dropped'),
            ),
            'bytes=3-5': response(b'def', status_code=206, content_range='bytes 3-5/12'),
            'bytes=6-11': response(b'ghijkl', status_code=206, content_range='bytes 6-11/12'),
        })
        gfile._make_download_session = session_factory

        with patch('gfile.gfile.threading.Event.wait', return_value=False):
            self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), body)
        requested_ranges = {
            request[1]['headers']['Range']
            for session in session_factory.sessions
            for request in session.requests
        }
        self.assertEqual(requested_ranges, {'bytes=0-5', 'bytes=3-5', 'bytes=6-11'})
        self.assertEqual(len(session_factory.sessions), 3)
        self.assertTrue(all(session.closed for session in session_factory.sessions))

    def test_accepts_disconnect_after_the_last_expected_byte(self):
        body = b'abcdef'
        probe = response(b'a', status_code=206, content_range='bytes 0-0/6')
        gfile = self.make_gfile([probe], download_threads=1, size='6B')
        session_factory = RangeSessionFactory({
            'bytes=0-5': response(
                body,
                status_code=206,
                content_range='bytes 0-5/6',
                stream_error=ConnectionError('late disconnect'),
            ),
        })
        gfile._make_download_session = session_factory

        self.assertEqual(gfile.download(), ['example.bin'])

        self.assertEqual(Path('example.bin').read_bytes(), body)
        self.assertEqual(len(session_factory.sessions), 1)


class ContentRangeTests(unittest.TestCase):
    def test_parses_valid_range(self):
        self.assertEqual(parse_content_range('bytes 4-9/10'), (4, 9, 10))

    def test_rejects_unknown_or_malformed_range(self):
        self.assertIsNone(parse_content_range('bytes 4-9/*'))
        self.assertIsNone(parse_content_range('not-a-range'))


class GigafileUrlTests(unittest.TestCase):
    def test_parses_jp_and_legacy_share_pages_using_jp(self):
        page = FakeResponse(b'')
        page.text = '<span class="dl_size">10B</span><span id="dl">example.bin</span>'
        for tld in ('jp', 'nu'):
            with self.subTest(tld=tld):
                gfile = GFile(f'https://12.gigafile.{tld}/example', mute=True)
                gfile.session = FakeSession([page])
                self.assertEqual(
                    gfile.parse_download_page(f'https://12.gigafile.{tld}/example'),
                    [('example.bin', '10B', 'example')],
                )
                self.assertEqual(gfile.session.requests[0][0], 'https://12.gigafile.jp/example')
                self.assertEqual(gfile.file_or_url, 'https://12.gigafile.jp/example')

    def test_jp_share_url_is_unchanged(self):
        url = 'https://12.gigafile.jp/example'
        self.assertEqual(normalize_gigafile_url(url), url)

    def test_normalizes_legacy_share_url(self):
        self.assertEqual(
            normalize_gigafile_url('https://12.gigafile.nu/example'),
            'https://12.gigafile.jp/example',
        )

    def test_does_not_rewrite_local_filename(self):
        filename = 'backup.gigafile.nu.zip'

        self.assertEqual(normalize_gigafile_url(filename), filename)


class CommandTests(unittest.TestCase):
    def test_aria2_defaults_to_continue(self):
        argv = ['gfile', 'download', 'https://1.gigafile.nu/share', '--aria2']
        with patch.object(sys, 'argv', argv), patch.object(cmd, 'GFile') as gfile_class:
            cmd.main()

        self.assertEqual(gfile_class.call_args.kwargs['aria2'], '-c -x10 -s10')
        self.assertEqual(gfile_class.call_args.kwargs['download_threads'], 4)


if __name__ == '__main__':
    unittest.main()
