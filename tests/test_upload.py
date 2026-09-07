import concurrent.futures
import io
import re
import tempfile
import threading
import tracemalloc
import unittest
from email.parser import BytesParser
from pathlib import Path
from unittest.mock import MagicMock, patch

from gfile.gfile import FileSlice, GFile


class AdmissionObserved(Exception):
    pass


class UploadAdmissionTests(unittest.TestCase):
    def test_refills_sender_slot_when_later_chunk_is_parked(self):
        gfile = GFile('unused', progress=False, mute=True, thread_num=4, window=2)
        gfile.failed = False
        gfile.current_chunk = 1
        gfile.next_send = 3
        # Chunk 1 is still sending while chunk 2 has overtaken it and parked.
        gfile.active_senders = 1
        admitted = threading.Event()
        errors = []

        def observe_admission(*args, **kwargs):
            admitted.set()
            raise AdmissionObserved

        def run_chunk():
            try:
                gfile.upload_chunk(3, 5)
            except BaseException as ex:
                errors.append(ex)

        with patch('builtins.open', side_effect=observe_admission):
            worker = threading.Thread(target=run_chunk)
            worker.start()
            try:
                self.assertTrue(admitted.wait(1), 'the available sender slot was not refilled')
            finally:
                with gfile._cond:
                    gfile.failed = True
                    gfile._cond.notify_all()
                worker.join(1)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], AdmissionObserved)
        self.assertEqual(gfile.next_send, 4)
        self.assertEqual(gfile.active_senders, 1)


def upload_response(data=None):
    response = MagicMock()
    response.__enter__.return_value = response
    response.json.return_value = {'status': 0} if data is None else data
    return response


def read_body(data):
    pieces = []
    while True:
        piece = data.read(16384)
        if not piece:
            return b''.join(pieces)
        pieces.append(piece)


class FileSliceTests(unittest.TestCase):
    def test_bounds_reads_and_rewinds(self):
        view = FileSlice(io.BytesIO(b'0123456789'), 2, 5, 3)
        self.assertEqual(view.len, 5)
        self.assertEqual(view.read(100), b'234')
        self.assertEqual(view.read(), b'56')
        self.assertEqual(view.read(), b'')
        view.rewind()
        self.assertEqual(view.read(2), b'23')
        self.assertEqual(view.len, 3)

    def test_truncated_source_raises_instead_of_looping(self):
        view = FileSlice(io.BytesIO(b'ab'), 0, 3, 10)
        self.assertEqual(view.read(), b'ab')
        with self.assertRaisesRegex(OSError, 'source ended'):
            view.read()


class StreamingUploadTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.path = Path(self.temp_dir.name) / 'example.bin'

    def make_gfile(self, chunk_size=300000):
        gfile = GFile(self.path, mute=True, chunk_size=chunk_size, max_retries=2)
        self.addCleanup(gfile.session.close)
        gfile.token = 'test-token'
        gfile.server = '1.gigafile.jp'
        gfile.failed = False
        return gfile

    def test_streams_exact_chunk_and_replays_after_partial_send(self):
        self.path.write_bytes(b'a' * 300000 + b'b' * 170000)
        gfile = self.make_gfile()
        gfile.current_chunk = gfile.next_send = 1
        prefixes = []
        bodies = []
        headers_seen = []

        def post(url, data, headers):
            headers_seen.append(headers)
            if not prefixes:
                prefixes.append(data.read(16384))
                raise ConnectionError('simulated partial send')
            body = read_body(data)
            self.assertEqual(len(body), data.len)
            bodies.append(body)
            return upload_response()

        with patch.object(gfile.session, 'post', side_effect=post), patch('gfile.gfile.time.sleep'), patch(
            'gfile.gfile.MultipartEncoder.to_string', side_effect=AssertionError('must stream')
        ):
            gfile.upload_chunk(1, 2)

        self.assertFalse(gfile.failed)
        self.assertEqual(gfile.current_chunk, 2)
        self.assertEqual(gfile.active_senders, 0)
        self.assertTrue(bodies[0].startswith(prefixes[0]))
        self.assertEqual(headers_seen[0], headers_seen[1])
        message = BytesParser().parsebytes(
            f'Content-Type: {headers_seen[-1]["content-type"]}\r\n\r\n'.encode() + bodies[0]
        )
        fields = {
            part.get_param('name', header='content-disposition'): part.get_payload(decode=True)
            for part in message.get_payload()
        }
        self.assertEqual(fields['file'], b'b' * 170000)
        self.assertEqual(fields['chunk'], b'1')
        self.assertEqual(fields['chunks'], b'2')

    def test_faster_later_chunk_cannot_commit_first(self):
        self.path.write_bytes(b'x' * 600000)
        gfile = self.make_gfile()
        later_started = threading.Event()
        commits = []

        def post(url, data, headers):
            prefix = data.read(16384)
            chunk = int(re.search(rb'name="chunk"\r\n\r\n(\d+)', prefix)[1])
            if chunk == 0:
                self.assertTrue(later_started.wait(2))
            else:
                later_started.set()
            body = prefix + read_body(data)
            self.assertEqual(len(body), data.len)
            commits.append(chunk)
            return upload_response()

        with patch.object(gfile.session, 'post', side_effect=post), concurrent.futures.ThreadPoolExecutor(
            max_workers=2
        ) as executor:
            futures = [executor.submit(gfile.upload_chunk, chunk, 2) for chunk in (0, 1)]
            for future in futures:
                future.result(timeout=5)
        self.assertEqual(commits, [0, 1])
        self.assertEqual(gfile.active_senders, 0)

    def test_rejected_chunk_does_not_advance_commit_order(self):
        self.path.write_bytes(b'abc')
        gfile = self.make_gfile()
        with patch.object(gfile.session, 'post', return_value=upload_response({'status': 1})):
            gfile.upload_chunk(0, 1)
        self.assertTrue(gfile.failed)
        self.assertEqual(gfile.current_chunk, 0)

    def test_large_chunk_does_not_require_chunk_sized_memory(self):
        size = 30 * 1024 * 1024
        with self.path.open('wb') as source:
            source.truncate(size)
        gfile = self.make_gfile(chunk_size=size)

        def post(url, data, headers):
            sent = 0
            while True:
                piece = data.read(16384)
                if not piece:
                    break
                sent += len(piece)
            self.assertEqual(sent, data.len)
            return upload_response()

        with patch.object(gfile.session, 'post', side_effect=post):
            tracemalloc.start()
            try:
                gfile.upload_chunk(0, 1)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
        self.assertLess(peak, 4 * 1024 * 1024)

    def test_upload_scheduler_limits_pending_chunks(self):
        self.path.write_bytes(b'x' * 100)
        gfile = self.make_gfile(chunk_size=1)
        gfile.data = {'url': 'https://1.gigafile.jp/test'}
        batches = []
        real_wait = concurrent.futures.wait

        def wait(futures, **kwargs):
            batches.append(len(futures))
            return real_wait(futures, **kwargs)

        page = MagicMock(text='var server = "1.gigafile.jp"')
        with patch.object(gfile.session, 'get', return_value=page), patch.object(
            gfile, 'upload_chunk'
        ) as upload_chunk, patch('gfile.gfile.concurrent.futures.wait', side_effect=wait):
            gfile.upload()
        self.assertEqual(upload_chunk.call_count, 100)
        self.assertTrue(batches)
        self.assertLessEqual(max(batches), gfile.thread_num)


if __name__ == '__main__':
    unittest.main()
