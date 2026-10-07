import csv
from pathlib import Path
import tempfile
import unittest

from mdpipe.harness import demo
from mdpipe.research import features

class ResearchTests(unittest.IsolatedAsyncioTestCase):
    async def test_feature_export_has_only_trusted_publications(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.sqlite"
            output = Path(directory) / "features.csv"
            report = await demo(path)
            exported = features(path, output)
            self.assertEqual(exported["rows"], report["metrics"]["published"])
            with output.open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(rows[0]["mid_price"], "10001")
            self.assertEqual(rows[0]["spread"], "2.00000000")
            self.assertEqual(rows[0]["top_imbalance"], "0")
            self.assertEqual(rows[-1]["sequence"], "49")
