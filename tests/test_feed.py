import json
import unittest

from mdpipe.feed import FeedError, normalize, units, decimal, SCALE
from mdpipe.simulated import payload

class FeedTests(unittest.TestCase):
    def test_fixed_point_is_exact(self):
        self.assertEqual(units("0.00000001"), 1)
        self.assertEqual(units("123.450000000000"), 12345000000)
        self.assertEqual(decimal(units("123.45")), "123.45000000")

    def test_bad_decimal_is_not_rounded(self):
        for value in ("0.000000001", "-1", "nan", "1e4", 1.0, "", "1.", "92233720369"):
            with self.subTest(value=value), self.assertRaises(FeedError):
                units(value)

    def test_snapshot_and_absolute_quantity(self):
        raw = payload("BTC-USD", 0, "snapshot", [("B", 100 * SCALE, 2 * SCALE), ("A", 101 * SCALE, SCALE)])
        packet = normalize(raw, "BTC-USD")
        self.assertEqual(packet.command(1), f"SNAP 1 0 2 B {100*SCALE} {2*SCALE} A {101*SCALE} {SCALE}")
        update = normalize(payload("BTC-USD", 1, "update", [("B", 100 * SCALE, 0)]), "BTC-USD")
        self.assertEqual(update.levels[0][2], 0)

    def test_control_channels_advance_envelope_sequence(self):
        self.assertEqual(normalize('{"channel":"heartbeats","sequence_num":999,"events":[]}', "BTC-USD").command(1), "TICK 1 999")
        self.assertEqual(normalize('{"channel":"subscriptions","sequence_num":0,"events":[]}', "BTC-USD").command(1), "TICK 1 0")
        with self.assertRaises(FeedError):
            normalize('{"channel":"subscriptions","events":[]}', "BTC-USD")

    def test_wrong_product_and_unknown_side_rejected(self):
        with self.assertRaises(FeedError):
            normalize(payload("ETH-USD", 0, "snapshot", []), "BTC-USD")
        msg = json.loads(payload("BTC-USD", 0, "snapshot", [("B", SCALE, SCALE)]))
        msg["events"][0]["updates"][0]["side"] = "buy"
        with self.assertRaises(FeedError):
            normalize(json.dumps(msg), "BTC-USD")

    def test_duplicate_snapshot_level_rejected(self):
        with self.assertRaises(FeedError):
            normalize(payload("BTC-USD", 0, "snapshot", [("B", SCALE, 0), ("B", SCALE, SCALE)]), "BTC-USD")

    def test_sequence_must_be_unsigned_integer(self):
        for value in (True, -1, 1.1, "1", 1 << 64, None):
            message = json.loads(payload("BTC-USD", 0, "snapshot", []))
            message["sequence_num"] = value
            with self.subTest(value=value), self.assertRaises(FeedError):
                normalize(json.dumps(message), "BTC-USD")

    def test_malformed_and_mixed_packet_rejected(self):
        for raw in ("{", "[]", '{"type":"error"}', '{"channel":"l2_data","sequence_num":1,"events":[]}'):
            with self.assertRaises(FeedError):
                normalize(raw, "BTC-USD")
        message = json.loads(payload("BTC-USD", 0, "snapshot", []))
        message["events"].append({"type": "update", "product_id": "BTC-USD", "updates": []})
        with self.assertRaises(FeedError):
            normalize(json.dumps(message), "BTC-USD")

    def test_batched_updates_share_one_sequence(self):
        message = json.loads(payload("BTC-USD", 1, "update", [("B", 100 * SCALE, SCALE)]))
        message["events"].append(json.loads(payload("BTC-USD", 1, "update", [("A", 101 * SCALE, SCALE)]))["events"][0])
        packet = normalize(json.dumps(message), "BTC-USD")
        self.assertEqual(len(packet.levels), 2)
        self.assertEqual(packet.sequence, 1)
