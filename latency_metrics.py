"""Latency CSV and statistics. Only standard-library dependencies."""
import csv
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path


METRICS = ("mqtt_latency_ms", "influx_write_latency_ms", "end_to_end_latency_ms")
FIELDS = (
    "received_utc", "device_id", "sequence", "sent_at_ms", "received_at_ms",
    "receive_at_ms", "receive_system_at_ms", "epoch_delta_ms", "system_delta_ms",
    "receiver_ntp_offset_ms", "receiver_ntp_rtt_ms", "receiver_ntp_age_ms",
    "receiver_clock_source", "sender_ntp_age_ms",
    "write_ack_at_ms", "ntp_synced", "write_success", "status",
    *METRICS, "subscriber_processing_ms", "write_attempt_ms", "error",
)


def measure(data, received_wall_ns, received_mono_ns, write, clock_info=None):
    """Cross-host times are estimates; local durations use perf_counter_ns."""
    sent = data.get("sent_at_ms")
    reference_ns = clock_info["epoch_ns"] if clock_info else received_wall_ns
    received_ms = reference_ns / 1_000_000
    elapsed_ms = (write["end_ns"] - received_mono_ns) / 1_000_000
    attempt_ms = (write["end_ns"] - write["start_ns"]) / 1_000_000
    status = []
    mqtt_ms = None
    epoch_delta_ms = None
    sync_age = data.get("ntp_sync_age_ms")
    valid_timestamp = (type(sent) in (int, float) and math.isfinite(sent)
                       and sent >= 1704067200000)
    if valid_timestamp:
        epoch_delta_ms = (reference_ns - int(sent * 1_000_000)) / 1_000_000
    if clock_info is not None and not clock_info["ready"]:
        status.append("receiver_not_ntp_synced")
    if not valid_timestamp:
        status.append("missing_or_invalid_timestamp")
    elif data.get("ntp_synced") is not True:
        status.append("sender_not_ntp_synced")
    elif type(sync_age) not in (int, float) or not math.isfinite(sync_age) or not 0 <= sync_age <= 20000:
        status.append("sender_sync_stale_or_missing")
    else:
        mqtt_ms = epoch_delta_ms
        if mqtt_ms < 0:
            status.append("sender_clock_ahead")
            mqtt_ms = None
        elif mqtt_ms > 5000:
            # Diagnostic gate, not proof of clock skew: could also be a queued packet.
            # Preserve the unmodified delta in CSV, but do not call this a valid sample.
            status.append("clock_or_delivery_delay_over_5s")
            mqtt_ms = None
    if clock_info is not None and not clock_info["ready"]:
        mqtt_ms = None
    # A PC wall-clock adjustment during the callback invalidates cross-host timing.
    wall_elapsed_ms = (write["end_wall_ns"] - received_wall_ns) / 1_000_000
    if abs(wall_elapsed_ms - elapsed_ms) > 50:
        status.append("receiver_clock_step")
        mqtt_ms = None
    if not write["ok"]:
        status.append("write_failed")
    return {
        "received_utc": datetime.fromtimestamp(reference_ns / 1e9, timezone.utc).isoformat(),
        "device_id": data.get("device_id", "unknown"),
        "sequence": data.get("sequence", ""),
        "sent_at_ms": sent if valid_timestamp else None,
        "received_at_ms": received_ms,
        "receive_at_ms": received_ms,
        "receive_system_at_ms": received_wall_ns / 1e6,
        "epoch_delta_ms": epoch_delta_ms,
        "system_delta_ms": (received_wall_ns - int(sent * 1_000_000))/1e6 if valid_timestamp else None,
        "receiver_ntp_offset_ms": clock_info["offset_ms"] if clock_info else 0,
        "receiver_ntp_rtt_ms": clock_info["rtt_ms"] if clock_info else None,
        "receiver_ntp_age_ms": clock_info["age_ms"] if clock_info else None,
        "receiver_clock_source": clock_info["source"] if clock_info else "system",
        "sender_ntp_age_ms": sync_age,
        "write_ack_at_ms": received_ms + elapsed_ms if write["ok"] else None,
        "ntp_synced": data.get("ntp_synced") is True,
        "write_success": write["ok"],
        "status": ";".join(status) or "ok",
        "mqtt_latency_ms": mqtt_ms,
        "influx_write_latency_ms": attempt_ms if write["ok"] else None,
        # Include JSON/validation/Point construction between receive and HTTP write.
        "end_to_end_latency_ms": mqtt_ms + elapsed_ms if mqtt_ms is not None and write["ok"] else None,
        "subscriber_processing_ms": (write["start_ns"] - received_mono_ns) / 1e6,
        "write_attempt_ms": attempt_ms,
        "error": write["error"],
    }


def percentile95(values):
    """Linear interpolation, matching pandas/NumPy's default percentile."""
    ordered = sorted(values)
    position = (len(ordered) - 1) * 0.95
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


class LatencyRecorder:
    def __init__(self, directory, summary_every=20):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        self.path = directory / f"latency_{run_id}.csv"
        self.summary_path = directory / f"latency_{run_id}_summary.csv"
        self.stream = self.path.open("x", newline="", encoding="utf-8-sig")
        self.writer = csv.DictWriter(self.stream, fieldnames=FIELDS)
        self.writer.writeheader()
        self.stream.flush()
        self.values = {metric: [] for metric in METRICS}
        self.count = 0
        self.summary_every = summary_every

    @property
    def valid_count(self):
        return len(self.values["end_to_end_latency_ms"])

    def record(self, row):
        self.writer.writerow({key: round(value, 3) if isinstance(value, float) else value
                              for key, value in row.items()})
        self.stream.flush()
        self.count += 1
        for metric in METRICS:
            if row[metric] is not None:
                self.values[metric].append(row[metric])
        formatted = [f"{row[m]:.3f}" if row[m] is not None else "N/A" for m in METRICS]
        delta = f"{row['epoch_delta_ms']:.3f}" if row['epoch_delta_ms'] is not None else "N/A"
        print(f"[EPOCH ms] sent_at_ms={row['sent_at_ms']} | "
              f"receive_at_ms={row['receive_at_ms']:.3f} | delta_ms={delta}")
        print(f"[PC CLOCK] system_epoch_ms={row['receive_system_at_ms']:.3f} | "
              f"ntp_offset_ms={row['receiver_ntp_offset_ms']:+.3f} | "
              f"ntp_rtt_ms={row['receiver_ntp_rtt_ms']} | sender_sync_age_ms={row['sender_ntp_age_ms']}")
        print(f"[LATENCY ms] seq={row['sequence']} | MQTT={formatted[0]} | "
              f"InfluxWrite={formatted[1]} | E2E={formatted[2]} | {row['status']}")
        print(f"[MAU DO] Total={self.count} | E2E hop le={self.valid_count}")
        if self.count % self.summary_every == 0:
            self.summary()

    def summary(self):
        rows = []
        print("\nDO TRE (ms) - MQTT/E2E phu thuoc dong bo dong ho")
        print(f"{'Metric':<24} {'N':>5} {'Min':>10} {'Avg':>10} {'Median':>10} {'P95':>10} {'Max':>10}")
        for metric, values in self.values.items():
            stats = ([min(values), statistics.mean(values), statistics.median(values),
                      percentile95(values), max(values)] if values else [None] * 5)
            print(f"{metric:<24} {len(values):>5} " + " ".join(
                f"{value:10.3f}" if value is not None else f"{'N/A':>10}" for value in stats))
            rows.append(dict(zip(("metric", "n", "min_ms", "avg_ms", "median_ms", "p95_ms", "max_ms"),
                                 (metric, len(values), *stats))))
        with self.summary_path.open("w", newline="", encoding="utf-8-sig") as stream:
            writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"[CSV] {self.path}\n[SUMMARY CSV] {self.summary_path}\n")

    def close(self):
        try:
            self.summary()
        finally:
            self.stream.close()
