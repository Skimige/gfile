import json
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def exception_types(error):
    """Describe nested transport failures without serializing URLs or secrets."""
    pending = [error]
    seen = set()
    names = set()
    while pending:
        current = pending.pop()
        if not isinstance(current, BaseException) or id(current) in seen:
            continue
        seen.add(id(current))
        names.add(type(current).__name__)
        pending.extend(current.args)
        pending.extend((current.__cause__, current.__context__))
    return sorted(names)


class UploadPerformanceLog:
    """Write JSONL asynchronously; transfer threads only update small counters."""

    def __init__(self, path, source, settings, warn, interval=5):
        path = Path(path)
        source = Path(source)
        if path.resolve() == source.resolve() or (path.exists() and path.samefile(source)):
            raise ValueError('Performance log must not be the upload source file.')
        self._file = path.open('a', encoding='utf-8')
        self._warn = warn
        self._interval = interval
        self._lock = threading.Lock()
        self._queue = queue.Queue()
        self._started = time.perf_counter()
        self._run_id = uuid.uuid4().hex
        self._closed = False
        self._write_failed = False
        self._chunks = {}
        self._committed_bytes = 0
        self._committed_chunks = 0
        self._yielded_bytes = 0
        self._retries = 0
        self._attempt_phase_seconds = {}
        self._sample_time = self._started
        self._sample_committed = 0
        self._sample_yielded = 0
        self.outcome = 'failed'
        self.event('upload_start', pid=os.getpid(), **settings)
        self._thread = threading.Thread(target=self._write_loop, name='gfile-performance', daemon=True)
        self._thread.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        outcome = 'cancelled' if isinstance(exc, KeyboardInterrupt) else self.outcome
        if exc is not None and outcome != 'cancelled':
            outcome = 'failed'
        self.close(outcome, exception_types(exc))

    def _emit(self, event, **fields):
        if not self._closed and not self._write_failed:
            self._queue.put({
                'schema': 1,
                'run_id': self._run_id,
                'time': datetime.now(timezone.utc).isoformat(),
                'elapsed_seconds': round(time.perf_counter() - self._started, 6),
                'event': event,
                **fields,
            })

    def event(self, event, **fields):
        with self._lock:
            self._emit(event, **fields)

    def phase(self, chunk, phase, **fields):
        with self._lock:
            if self._closed or self._write_failed:
                return
            now = time.perf_counter()
            state = self._chunks.setdefault(chunk, {
                'phase': phase, 'since': now, 'phase_seconds': {},
                'attempt': 0, 'body_bytes_yielded': 0,
            })
            previous = state['phase']
            elapsed = now - state['since']
            state['phase_seconds'][previous] = state['phase_seconds'].get(previous, 0) + elapsed
            state.update(phase=phase, since=now, **fields)
            self._emit('upload_phase', chunk=chunk, phase=phase,
                       previous_phase=previous, previous_phase_seconds=round(elapsed, 6), **fields)

    def start_attempt(self, chunk, attempt, body_bytes):
        self.phase(chunk, 'sending', attempt=attempt, body_bytes=body_bytes, body_bytes_yielded=0)
        with self._lock:
            if chunk in self._chunks:
                self._chunks[chunk]['phase_seconds'] = {}

    def yielded(self, chunk, count):
        with self._lock:
            if self._closed or self._write_failed:
                return
            self._chunks[chunk]['body_bytes_yielded'] += count
            self._yielded_bytes += count

    @staticmethod
    def _durations(state, now):
        durations = dict(state['phase_seconds'])
        durations[state['phase']] = durations.get(state['phase'], 0) + now - state['since']
        return {key: round(value, 6) for key, value in durations.items()}

    def finish_attempt(self, chunk, outcome, error=None, retry_delay=0, http_status=None):
        with self._lock:
            if self._closed or self._write_failed:
                return
            state = self._chunks[chunk]
            if retry_delay:
                self._retries += 1
            durations = self._durations(state, time.perf_counter())
            for phase, duration in durations.items():
                self._attempt_phase_seconds[phase] = self._attempt_phase_seconds.get(phase, 0) + duration
            self._emit(
                'upload_attempt', chunk=chunk, attempt=state['attempt'], outcome=outcome,
                phase=state['phase'], phase_seconds=durations,
                body_bytes=state['body_bytes'], body_bytes_yielded=state['body_bytes_yielded'],
                error_types=exception_types(error), retry_delay_seconds=retry_delay,
                http_status=http_status,
            )

    def committed(self, chunk, raw_bytes):
        with self._lock:
            if self._closed or self._write_failed:
                return
            self._committed_bytes += raw_bytes
            self._committed_chunks += 1
            self._chunks.pop(chunk, None)
            self._emit('upload_commit', chunk=chunk, raw_bytes=raw_bytes,
                       committed_bytes=self._committed_bytes)

    def _snapshot(self):
        now = time.perf_counter()
        elapsed = now - self._started
        interval = max(now - self._sample_time, 1e-9)
        snapshot = {
            'committed_bytes': self._committed_bytes,
            'committed_chunks': self._committed_chunks,
            'committed_bytes_per_second': round(self._committed_bytes / max(elapsed, 1e-9), 2),
            'body_bytes_yielded': self._yielded_bytes,
            'retries': self._retries,
            'attempt_phase_seconds': {key: round(value, 6) for key, value in self._attempt_phase_seconds.items()},
            'interval_seconds': round(interval, 6),
            'interval_committed_bytes_per_second': round((self._committed_bytes - self._sample_committed) / interval, 2),
            'interval_body_bytes_yielded_per_second': round((self._yielded_bytes - self._sample_yielded) / interval, 2),
            'active_chunks': [
                {'chunk': chunk, **{key: value for key, value in state.items() if key not in ('since', 'phase_seconds')},
                 'phase_age_seconds': round(now - state['since'], 6),
                 'phase_seconds': self._durations(state, now)}
                for chunk, state in sorted(self._chunks.items())
            ],
        }
        self._sample_time = now
        self._sample_committed = self._committed_bytes
        self._sample_yielded = self._yielded_bytes
        return snapshot

    def _write_loop(self):
        deadline = time.monotonic() + self._interval
        try:
            while True:
                try:
                    item = self._queue.get(timeout=max(0, deadline - time.monotonic()))
                except queue.Empty:
                    item = None
                if item is False:
                    break
                if item is not None:
                    self._file.write(json.dumps(item, ensure_ascii=True) + '\n')
                    self._file.flush()
                if time.monotonic() >= deadline:
                    with self._lock:
                        self._emit('upload_snapshot', **self._snapshot())
                    deadline = time.monotonic() + self._interval
        except OSError:
            self._write_failed = True
        finally:
            try:
                self._file.close()
            except OSError:
                self._write_failed = True

    def close(self, outcome, errors=None):
        with self._lock:
            if self._closed:
                return
            self._emit('upload_end', outcome=outcome, error_types=errors or [], **self._snapshot())
            self._closed = True
            self._queue.put(False)
        self._thread.join()
        if self._write_failed:
            self._warn('Performance logging stopped after a log write failure; transfer was not interrupted.')
