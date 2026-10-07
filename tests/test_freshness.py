from pathlib import Path
import tempfile
import time
import unittest

from mdpipe.journal import Journal, Recorder
from mdpipe.native import NativeBook
from mdpipe.pipeline import Config, Pipeline
from mdpipe.replay import replay
from mdpipe.simulated import SimulatedExchange

class FreshnessTests(unittest.IsolatedAsyncioTestCase):
    async def test_watchdog_closes_gate_during_slow_disk_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.sqlite"
            journal = Journal(path, "BTC-USD")
            original = journal.append
            def stall_once(event):
                if event.get("command", "").startswith("UPD 1 1 "):
                    time.sleep(.08)
                return original(event)
            journal.append = stall_once
            recorder = Recorder(journal)
            states, quotes = [], []
            try:
                async with NativeBook() as core:
                    pipeline = Pipeline(core, recorder,
                        Config(max_lag_seconds=.03, max_reconnects=0, retry_seconds=0),
                        on_quote=quotes.append, on_state=states.append)
                    report = await pipeline.run(SimulatedExchange(interval=.005))
                    self.assertEqual(report["metrics"]["invalidations"].get("stale_processing"), 1)
                    self.assertEqual([quote["sequence"] for quote in quotes], [0])
                    self.assertEqual(states[0]["reason"], "stale_processing")
            finally:
                await recorder.close()
            restored = await replay(path)
            self.assertEqual(restored["last_trusted"], report["last_trusted"])
