import asyncio
from pathlib import Path
import tempfile
import time
import unittest

from mdpipe.harness import fault_suite
from mdpipe.journal import Journal, JournalError, Recorder, records
from mdpipe.native import NativeBook
from mdpipe.pipeline import BoundedQueue, Config, Envelope, Pipeline, Recover
from mdpipe.replay import replay
from mdpipe.simulated import SimulatedExchange, payload

class QueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_byte_and_message_limits_and_accounting(self):
        queue = BoundedQueue(2, 5)
        frame = Envelope("abc", 1, 0, 3)
        queue.put(frame)
        with self.assertRaises(Recover):
            queue.put(frame)
        await queue.get()
        self.assertEqual(queue.bytes, 0)
        queue.put(Envelope("a", 1, 0, 1))
        queue.put(Envelope("b", 1, 0, 1))
        with self.assertRaises(Recover):
            queue.put(Envelope("c", 1, 0, 1))

class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_five_fault_scenarios_recover_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            report = await fault_suite(Path(directory) / "suite")
            self.assertEqual(report["passed"], 5)
            for scenario in report["scenarios"]:
                self.assertFalse(scenario["final"]["trusted"])
                self.assertGreater(scenario["verified_publications"], 0)
                self.assertEqual(scenario["replay"]["integrity"], "verified")

    async def test_missing_snapshot_times_out_even_with_heartbeats(self):
        class Source:
            sequence = -1
            async def __aenter__(self): return self
            async def __aexit__(self, *_): pass
            async def recv(self):
                await asyncio.sleep(.003)
                self.sequence += 1
                return f'{{"channel":"heartbeats","sequence_num":{self.sequence},"events":[]}}'
        with tempfile.TemporaryDirectory() as directory:
            recorder = Recorder(Journal(Path(directory) / "a.sqlite", "BTC-USD"))
            try:
                async with NativeBook() as core:
                    pipeline = Pipeline(core, recorder, Config(snapshot_seconds=.03, max_reconnects=0, retry_seconds=0))
                    result = await pipeline.run(lambda _: Source())
                    self.assertEqual(result["metrics"]["invalidations"].get("snapshot_timeout"), 1)
                    self.assertEqual(result["metrics"]["published"], 0)
            finally:
                await recorder.close()

    async def test_idle_connection_is_not_trusted(self):
        class Source:
            async def __aenter__(self): return self
            async def __aexit__(self, *_): pass
            async def recv(self): await asyncio.Future()
        with tempfile.TemporaryDirectory() as directory:
            recorder = Recorder(Journal(Path(directory) / "a.sqlite", "BTC-USD"))
            try:
                async with NativeBook() as core:
                    pipeline = Pipeline(core, recorder, Config(idle_seconds=.02, max_reconnects=0, retry_seconds=0))
                    result = await pipeline.run(lambda _: Source())
                    self.assertEqual(result["metrics"]["invalidations"].get("idle_timeout"), 1)
                    self.assertFalse(pipeline.gate_open)
            finally:
                await recorder.close()

    async def test_disk_commit_failure_stops_publication_and_poisoned_recorder(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.sqlite"
            journal = Journal(path, "BTC-USD")
            original = journal.append
            def fail_snapshot(event):
                if event.get("command", "").startswith("SNAP"):
                    raise JournalError("injected disk error")
                return original(event)
            journal.append = fail_snapshot
            recorder = Recorder(journal)
            quotes = []
            states = []
            try:
                async with NativeBook() as core:
                    pipeline = Pipeline(core, recorder, Config(stop_after=5), on_quote=quotes.append, on_state=states.append)
                    with self.assertRaises(JournalError):
                        await pipeline.run(SimulatedExchange())
                    self.assertEqual(quotes, [])
                    self.assertTrue(recorder.failed)
                    self.assertFalse((await core.request("STATUS"))["trusted"])
            finally:
                await recorder.close()
            # The journal remains a replayable prefix, without an unrecorded SNAP.
            restored = await replay(path)
            self.assertIsNone(restored["last_trusted"])

    async def test_disconnect_during_commit_cannot_publish_pending_quote(self):
        class Source:
            index = 0
            async def __aenter__(self): return self
            async def __aexit__(self, *_): pass
            async def recv(self):
                self.index += 1
                await asyncio.sleep(.005)
                if self.index == 1:
                    return payload("BTC-USD", 0, "snapshot", [("B", 100, 2), ("A", 110, 3)])
                if self.index == 2:
                    return payload("BTC-USD", 1, "update", [("B", 100, 9)])
                raise ConnectionError("connection lost while disk writer is busy")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.sqlite"
            journal = Journal(path, "BTC-USD")
            original = journal.append
            def slow_update(event):
                if event.get("command", "").startswith("UPD"):
                    time.sleep(.05)
                return original(event)
            journal.append = slow_update
            recorder = Recorder(journal)
            quotes = []
            states = []
            try:
                async with NativeBook() as core:
                    pipeline = Pipeline(core, recorder, Config(max_reconnects=0, retry_seconds=0), on_quote=quotes.append, on_state=states.append)
                    result = await pipeline.run(lambda _: Source())
                    self.assertEqual([quote["sequence"] for quote in quotes], [0])
                    self.assertFalse(pipeline.gate_open)
                    self.assertEqual(states[0]["trusted"], False)
                    self.assertEqual(states[0]["reason"], "disconnect")
            finally:
                await recorder.close()
            restored = await replay(path)
            self.assertEqual(restored["last_trusted"], result["last_trusted"])
            self.assertEqual(restored["final"], result["final"])

    async def test_queue_byte_overflow_prevents_snapshot_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = Recorder(Journal(Path(directory) / "a.sqlite", "BTC-USD"))
            try:
                async with NativeBook() as core:
                    pipeline = Pipeline(core, recorder, Config(queue_bytes=8, max_reconnects=0, retry_seconds=0))
                    result = await pipeline.run(SimulatedExchange())
                    self.assertEqual(result["metrics"]["invalidations"].get("queue_overflow"), 1)
                    self.assertEqual(result["metrics"]["published"], 0)
                    self.assertLessEqual(result["metrics"]["queue_peak_bytes"], 8)
            finally:
                await recorder.close()

    async def test_duration_shutdown_journal_remains_replayable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.sqlite"
            recorder = Recorder(Journal(path, "BTC-USD"))
            try:
                async with NativeBook() as core:
                    pipeline = Pipeline(core, recorder, Config(duration_seconds=.03))
                    result = await pipeline.run(SimulatedExchange(interval=.002))
                    self.assertFalse(pipeline.gate_open)
            finally:
                await recorder.close()
            restored = await replay(path)
            self.assertEqual(restored["final"], result["final"])
            self.assertEqual(restored["last_trusted"], result["last_trusted"])
