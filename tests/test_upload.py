import threading
import unittest
from unittest.mock import patch

from gfile.gfile import GFile


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

        with patch('gfile.gfile.split_file', side_effect=observe_admission):
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


if __name__ == '__main__':
    unittest.main()
