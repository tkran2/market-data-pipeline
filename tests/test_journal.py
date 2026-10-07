from pathlib import Path
import sqlite3
import tempfile
import unittest

from mdpipe.harness import demo
from mdpipe.journal import Journal, JournalError, records
from mdpipe.replay import replay

class JournalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "capture.sqlite"

    def tearDown(self):
        self.directory.cleanup()

    def test_append_and_committed_prefix_readable(self):
        journal = Journal(self.path, "BTC-USD")
        journal.append({"kind": "fault", "fault": "test"})
        # A second reader sees committed rows while the original WAL writer is open.
        self.assertEqual(len(list(records(self.path))), 2)
        journal.close()

    def test_existing_journal_not_overwritten(self):
        journal = Journal(self.path, "BTC-USD")
        journal.close()
        with self.assertRaises(JournalError):
            Journal(self.path, "BTC-USD")

    def test_corrupted_row_is_rejected(self):
        journal = Journal(self.path, "BTC-USD")
        journal.append({"kind": "fault", "fault": "test"})
        journal.close()
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE events SET payload='{}' WHERE id=2")
        connection.commit()
        connection.close()
        with self.assertRaises(JournalError):
            list(records(self.path))

    def test_missing_middle_row_is_rejected(self):
        journal = Journal(self.path, "BTC-USD")
        for i in range(3):
            journal.append({"kind": "fault", "fault": str(i)})
        journal.close()
        connection = sqlite3.connect(self.path)
        connection.execute("DELETE FROM events WHERE id=2")
        connection.commit()
        connection.close()
        with self.assertRaises(JournalError):
            list(records(self.path))

    def test_storage_budget_fails_without_committing(self):
        journal = Journal(self.path, "BTC-USD")
        journal.max_bytes = 0
        with self.assertRaises(JournalError):
            journal.append({"kind": "fault"})
        journal.close()
        self.assertEqual(len(list(records(self.path))), 1)

class ReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_offline_capture_replays_exactly_at_multiple_speeds(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.sqlite"
            captured = await demo(path)
            for speed in (0, 1000):
                restored = await replay(path, speed)
                self.assertEqual(restored["last_trusted"], captured["last_trusted"])
                self.assertEqual(restored["final"], captured["final"])
                self.assertEqual(restored["integrity"], "verified")
