import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from gfile import cmd
from gfile.gfile import GFile
from gfile.performance import UploadPerformanceLog, exception_types


class PerformanceLogTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.source = Path(directory.name) / 'private-source.bin'
        self.source.write_bytes(b'x' * 300000)
        self.path = Path(directory.name) / 'performance.jsonl'

    def records(self):
        return [json.loads(line) for line in self.path.read_text(encoding='utf-8').splitlines()]

    def test_phase_durations_and_summary(self):
        now = [0]
        with patch('gfile.performance.time.perf_counter', side_effect=lambda: now[0]), UploadPerformanceLog(
            self.path, self.source, {}, self.fail,
        ) as log:
            log.phase(1, 'waiting_admission')
            now[0] = 2
            log.start_attempt(1, 1, 100)
            log.yielded(1, 90)
            now[0] = 5
            log.phase(1, 'waiting_turn')
            now[0] = 12
            log.phase(1, 'sending_tail')
            log.yielded(1, 10)
            now[0] = 13
            log.phase(1, 'waiting_response')
            now[0] = 19
            log.finish_attempt(1, 'accepted', http_status=200)
            log.committed(1, 80)
            log.outcome = 'succeeded'
        records = self.records()
        attempt = next(item for item in records if item['event'] == 'upload_attempt')
        self.assertEqual(attempt['phase_seconds'], {
            'sending': 3, 'waiting_turn': 7, 'sending_tail': 1, 'waiting_response': 6,
        })
        self.assertEqual(attempt['body_bytes_yielded'], 100)
        summary = records[-1]
        self.assertEqual(summary['outcome'], 'succeeded')
        self.assertEqual(summary['committed_bytes'], 80)
        self.assertEqual(summary['active_chunks'], [])
        self.assertEqual(summary['attempt_phase_seconds'], attempt['phase_seconds'])
        self.assertFalse(log._thread.is_alive())

    def test_snapshots_continue_while_chunk_is_waiting(self):
        snapshot = threading.Event()
        original_emit = UploadPerformanceLog._emit

        def emit(log, event, **fields):
            original_emit(log, event, **fields)
            if event == 'upload_snapshot':
                snapshot.set()

        with patch.object(UploadPerformanceLog, '_emit', emit), UploadPerformanceLog(
            self.path, self.source, {}, self.fail, interval=0.01,
        ) as log:
            log.phase(1, 'waiting_turn')
            self.assertTrue(snapshot.wait(2))
        snapshots = [item for item in self.records() if item['event'] == 'upload_snapshot']
        self.assertEqual(snapshots[0]['active_chunks'][0]['phase'], 'waiting_turn')
        self.assertGreater(snapshots[0]['active_chunks'][0]['phase_age_seconds'], 0)

    def test_append_and_ignore_updates_after_close(self):
        for _ in range(2):
            with UploadPerformanceLog(self.path, self.source, {}, self.fail) as log:
                pass
            log.phase(99, 'sending')
            log.yielded(99, 12)
        records = self.records()
        self.assertEqual([item['event'] for item in records], ['upload_start', 'upload_end'] * 2)
        self.assertNotEqual(records[0]['run_id'], records[2]['run_id'])

    def test_rejects_log_path_that_is_source(self):
        with self.assertRaisesRegex(ValueError, 'upload source'):
            UploadPerformanceLog(self.source, self.source, {}, self.fail)
        self.assertEqual(self.source.stat().st_size, 300000)

    def test_nested_errors_do_not_expose_messages(self):
        error = ConnectionError('https://user:password@host/file?token=secret', TimeoutError('secret'))
        self.assertEqual(exception_types(error), ['ConnectionError', 'TimeoutError'])

    def test_log_write_failure_does_not_fail_transfer(self):
        output = MagicMock()
        output.write.side_effect = OSError('disk full')
        warning = MagicMock()
        with patch.object(Path, 'open', return_value=output), UploadPerformanceLog(
            self.path, self.source, {}, warning,
        ) as log:
            log.phase(1, 'sending')
        warning.assert_called_once()
        output.close.assert_called_once()
        self.assertFalse(log._thread.is_alive())

    def test_upload_retry_logs_bytes_outcomes_and_no_secrets(self):
        gfile = GFile(self.source, performance_log=self.path, mute=True, max_retries=2, key='private-key')
        self.addCleanup(gfile.session.close)
        page = MagicMock(text='var server = "86.gigafile.nu"')
        response = MagicMock(status_code=200)
        response.__enter__.return_value = response
        response.json.return_value = {'status': 0, 'url': 'https://86.gigafile.jp/private-share'}
        attempts = []

        def post(url, data, headers):
            attempts.append(data.read(16384))
            if len(attempts) == 1:
                raise ConnectionError('private-token', TimeoutError('private-password'))
            while data.read(16384):
                pass
            return response

        with patch.object(gfile.session, 'get', return_value=page), patch.object(
            gfile.session, 'post', side_effect=post,
        ), patch('gfile.gfile.time.sleep'):
            gfile.upload()
        records = self.records()
        attempts = [item for item in records if item['event'] == 'upload_attempt']
        self.assertEqual([item['outcome'] for item in attempts], ['failed', 'accepted'])
        self.assertEqual(attempts[0]['phase'], 'sending')
        self.assertEqual(attempts[0]['error_types'], ['ConnectionError', 'TimeoutError'])
        self.assertEqual(attempts[0]['retry_delay_seconds'], 1)
        self.assertEqual(attempts[1]['body_bytes_yielded'], attempts[1]['body_bytes'])
        self.assertEqual(attempts[1]['phase'], 'waiting_response')
        self.assertEqual(records[-1]['outcome'], 'succeeded')
        self.assertEqual(records[-1]['retries'], 1)
        self.assertEqual(records[-1]['committed_bytes'], 300000)
        self.assertIn({'host': '86.gigafile.nu'}, [{'host': r['host']} for r in records if 'host' in r])
        text = self.path.read_text(encoding='utf-8')
        self.assertNotIn('private-', text)
        self.assertIsNone(gfile._performance)

    def test_cancelled_upload_closes_log(self):
        gfile = GFile(self.source, performance_log=self.path, mute=True)
        self.addCleanup(gfile.session.close)
        with patch.object(gfile, '_upload', side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            gfile.upload()
        self.assertEqual(self.records()[-1]['outcome'], 'cancelled')
        self.assertIsNone(gfile._performance)

    def test_failed_upload_has_no_success_summary_or_final_backoff(self):
        gfile = GFile(self.source, performance_log=self.path, mute=True, max_retries=1)
        self.addCleanup(gfile.session.close)
        page = MagicMock(text='var server = "86.gigafile.nu"')
        with patch.object(gfile.session, 'get', return_value=page), patch.object(
            gfile.session, 'post', side_effect=TimeoutError('private-message'),
        ), patch('gfile.gfile.time.sleep') as sleep:
            gfile.upload()
        sleep.assert_not_called()
        self.assertEqual(self.records()[-1]['outcome'], 'failed')
        self.assertEqual(self.records()[-1]['committed_bytes'], 0)

    def test_cli_passes_log_path_to_upload(self):
        argv = ['gfile', 'upload', 'input.bin', '--performance-log', str(self.path)]
        with patch.object(sys, 'argv', argv), patch.object(cmd, 'GFile') as gfile:
            cmd.main()
        self.assertEqual(gfile.call_args.kwargs['performance_log'], str(self.path))

    def test_cli_rejects_log_option_for_download(self):
        argv = ['gfile', 'download', 'https://1.gigafile.jp/share', '--performance-log', str(self.path)]
        with patch.object(sys, 'argv', argv), patch('sys.stderr'), self.assertRaises(SystemExit) as error:
            cmd.main()
        self.assertEqual(error.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
