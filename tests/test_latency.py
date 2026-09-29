"""Synthetic unit-test inputs only; never write experiment results."""
import contextlib
import csv
import io
import tempfile
import unittest

from latency_metrics import LatencyRecorder, measure, percentile95
from latency_clock import EpochClock, ntp_bytes, parse_reply, unix_ns


class LatencyTests(unittest.TestCase):
    def setUp(self):
        self.wall = 1760000000000000000
        self.mono = 1000000000
        self.data = {"sent_at_ms": self.wall // 1000000 - 40,
                     "ntp_synced": True, "ntp_sync_age_ms": 5000,
                     "device_id": "test", "sequence": 1}
        self.write = dict(ok=True, start_ns=self.mono + 3000000,
                          end_ns=self.mono + 13000000,
                          end_wall_ns=self.wall + 13000000, error="")

    def row(self):
        return measure(self.data, self.wall, self.mono, self.write)

    def test_e2e_includes_subscriber_processing(self):
        row = self.row()
        self.assertEqual(row["mqtt_latency_ms"], 40)
        self.assertEqual(row["influx_write_latency_ms"], 10)
        self.assertEqual(row["subscriber_processing_ms"], 3)
        self.assertEqual(row["end_to_end_latency_ms"], 53)
        self.assertEqual(row["status"], "ok")

    def test_unsynced_or_missing_sender_still_allows_local_write_timing(self):
        for data in ({}, {"sent_at_ms": True, "ntp_synced": True},
                     {**self.data, "ntp_synced": False}):
            with self.subTest(data=data):
                row = measure(data, self.wall, self.mono, self.write)
                self.assertIsNone(row["mqtt_latency_ms"])
                self.assertIsNone(row["end_to_end_latency_ms"])
                self.assertEqual(row["influx_write_latency_ms"], 10)

    def test_negative_latency_is_not_clamped_or_counted(self):
        self.data["sent_at_ms"] = self.wall // 1000000 + 1
        row = self.row()
        self.assertIn("sender_clock_ahead", row["status"])
        self.assertIsNone(row["mqtt_latency_ms"])
        self.assertIsNone(row["end_to_end_latency_ms"])

    def test_wall_clock_step_does_not_corrupt_write_duration(self):
        self.write["end_wall_ns"] += 1000000000
        row = self.row()
        self.assertEqual(row["status"], "receiver_clock_step")
        self.assertIsNone(row["end_to_end_latency_ms"])
        self.assertEqual(row["influx_write_latency_ms"], 10)

    def test_write_failure_keeps_mqtt_but_not_success_statistics(self):
        self.write.update(ok=False, error="HTTP 500 test")
        row = self.row()
        self.assertEqual(row["mqtt_latency_ms"], 40)
        self.assertIsNone(row["influx_write_latency_ms"])
        self.assertIsNone(row["end_to_end_latency_ms"])
        self.assertIsNone(row["write_ack_at_ms"])
        self.assertEqual(row["write_attempt_ms"], 10)

    def test_p95_interpolates_and_handles_one_sample(self):
        self.assertEqual(percentile95([9]), 9)
        self.assertAlmostEqual(percentile95([100, 40]), 97)

    def test_80_second_delta_is_preserved_but_not_counted_as_valid(self):
        self.data["sent_at_ms"] -= 80000
        row = self.row()
        self.assertEqual(row["epoch_delta_ms"], 80040)
        self.assertIsNone(row["end_to_end_latency_ms"])
        self.assertIn("clock_or_delivery_delay_over_5s", row["status"])

    def test_independent_ntp_reference_corrects_pc_clock_not_mqtt_samples(self):
        reference = dict(ready=True, epoch_ns=self.wall, offset_ms=-80000,
                         rtt_ms=20, age_ms=100, source="test_ntp")
        self.write["end_wall_ns"] += 80000000000
        row = measure(self.data, self.wall + 80000000000, self.mono, self.write, reference)
        self.assertEqual(row["system_delta_ms"], 80040)
        self.assertEqual(row["epoch_delta_ms"], 40)
        self.assertEqual(row["end_to_end_latency_ms"], 53)
        self.assertEqual(row["status"], "ok")
        reference["ready"] = False
        row = measure(self.data, self.wall + 80000000000, self.mono, self.write, reference)
        self.assertIsNone(row["mqtt_latency_ms"])
        self.assertEqual(row["influx_write_latency_ms"], 10)

    def test_sender_sync_must_be_recent(self):
        self.data["ntp_sync_age_ms"] = 20001
        self.assertEqual(self.row()["status"], "sender_sync_stale_or_missing")
        self.assertIsNone(self.row()["mqtt_latency_ms"])

    def test_ntp_epoch_and_offset_without_timezone_adjustment(self):
        # Windows 80 s ahead; symmetric 10 ms network each way, server takes 1 ms.
        t1 = self.wall + 80000000000
        t4 = t1 + 21000000
        packet = bytearray(48)
        packet[0], packet[1] = 0x24, 1
        origin = ntp_bytes(t1)
        packet[24:32] = origin
        packet[32:40] = ntp_bytes(self.wall + 10000000)
        packet[40:48] = ntp_bytes(self.wall + 11000000)
        offset, rtt = parse_reply(packet, origin, t1, t4, 21000000)
        self.assertAlmostEqual(offset / 1e6, -80000, places=3)
        self.assertAlmostEqual(rtt / 1e6, 20, places=3)
        self.assertLessEqual(abs(unix_ns(ntp_bytes(self.wall)) - self.wall), 1)
        packet[24] ^= 1
        with self.assertRaises(ValueError):
            parse_reply(packet, origin, t1, t4, 21000000)

    def test_ntp_reference_uses_monotonic_clock_and_expires(self):
        clock = EpochClock()
        clock.reference = dict(epoch_ns=self.wall, mono_ns=self.mono,
                               rtt_ns=20000000, server="test_ntp")
        snap = clock.snapshot(self.mono + 1000000000, self.wall + 81000000000)
        self.assertEqual(snap["epoch_ns"], self.wall + 1000000000)
        self.assertEqual(snap["offset_ms"], -80000)
        self.assertTrue(snap["ready"])
        self.assertFalse(clock.snapshot(self.mono + 91000000000, self.wall)["ready"])

    def test_csv_flushed_and_summary_uses_per_metric_counts(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            recorder = LatencyRecorder(directory)
            recorder.record(self.row())
            self.write.update(ok=False, error="test failure")
            recorder.record(self.row())
            with recorder.path.open(encoding="utf-8-sig", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 2)  # Already flushed before close.
            self.assertEqual(rows[1]["end_to_end_latency_ms"], "")
            self.assertEqual(recorder.valid_count, 1)
            recorder.close()
            with recorder.summary_path.open(encoding="utf-8-sig", newline="") as stream:
                summary = {r["metric"]: r for r in csv.DictReader(stream)}
            self.assertEqual(summary["mqtt_latency_ms"]["n"], "2")
            self.assertEqual(summary["end_to_end_latency_ms"]["n"], "1")
            empty = LatencyRecorder(directory)
            empty.close()
            with empty.summary_path.open(encoding="utf-8-sig", newline="") as stream:
                self.assertTrue(all(r["n"] == "0" and r["avg_ms"] == ""
                                    for r in csv.DictReader(stream)))


if __name__ == "__main__":
    unittest.main()
