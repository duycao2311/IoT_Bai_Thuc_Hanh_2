"""UTC epoch reference for latency only; never changes the Windows clock."""
import socket
import struct
import threading
import time

NTP_UNIX_DELTA = 2208988800


def ntp_bytes(unix_ns):
    seconds, nanos = divmod(unix_ns, 1_000_000_000)
    return struct.pack("!II", seconds + NTP_UNIX_DELTA, (nanos << 32) // 1_000_000_000)


def unix_ns(raw):
    seconds, fraction = struct.unpack("!II", raw)
    return (seconds - NTP_UNIX_DELTA) * 1_000_000_000 + (fraction * 1_000_000_000 >> 32)


def parse_reply(packet, request_timestamp, t1, t4, elapsed_ns):
    if len(packet) < 48 or packet[0] & 7 != 4 or packet[0] >> 6 == 3:
        raise ValueError("NTP response invalid/unsynchronized")
    if not 1 <= packet[1] <= 15 or packet[24:32] != request_timestamp:
        raise ValueError("NTP stratum/originate timestamp invalid")
    if abs((t4 - t1) - elapsed_ns) > 50_000_000:
        raise ValueError("Windows clock changed during NTP probe")
    t2, t3 = unix_ns(packet[32:40]), unix_ns(packet[40:48])
    rtt_ns = (t4 - t1) - (t3 - t2)
    if t3 < t2 or not 0 <= rtt_ns <= 2_000_000_000:
        raise ValueError("NTP round trip invalid/too long")
    offset_ns = ((t2 - t1) + (t3 - t4)) // 2
    return offset_ns, rtt_ns


def probe(server):
    address = socket.getaddrinfo(server, 123, socket.AF_INET, socket.SOCK_DGRAM)[0][4]
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2)
        sock.connect(address)
        packet = bytearray(48)
        packet[0] = 0x23  # NTP v4 client; UTC, no timezone conversion.
        m1 = time.perf_counter_ns()
        t1 = time.time_ns()
        packet[40:48] = ntp_bytes(t1)
        sock.send(packet)
        reply = sock.recv(512)
        m4 = time.perf_counter_ns()
        t4 = time.time_ns()
    offset, rtt = parse_reply(reply, packet[40:48], t1, t4, m4 - m1)
    return dict(epoch_ns=t4 + offset, mono_ns=m4, offset_ns=offset,
                rtt_ns=rtt, server=server)


class EpochClock:
    """NTP-calibrated epoch, advanced with monotonic elapsed time between probes."""
    def __init__(self):
        self.reference = None
        self.error = "not synchronized"
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None

    def synchronize(self):
        samples = []
        for server in ("time.cloudflare.com", "time.cloudflare.com", "pool.ntp.org"):
            try:
                samples.append(probe(server))
            except (OSError, ValueError) as exc:
                self.error = str(exc)
        if samples:
            # Prefer the least network delay; correction comes from independent NTP,
            # never from observed MQTT latency or a hard-coded 80-second subtraction.
            reference = min(samples, key=lambda sample: sample["rtt_ns"])
            with self.lock:
                self.reference = reference
            print(f"[PC NTP] server={reference['server']} | "
                  f"offset_to_system_ms={reference['offset_ns']/1e6:+.3f} | "
                  f"round_trip_ms={reference['rtt_ns']/1e6:.3f}")
            return True
        print(f"[PC NTP] Chua dong bo: {self.error}; MQTT/E2E = N/A neu tham chieu het han.")
        return False

    def start(self):
        self.synchronize()
        self.thread = threading.Thread(target=self._refresh, daemon=True)
        self.thread.start()

    def _refresh(self):
        while not self.stop_event.wait(30):
            self.synchronize()

    def snapshot(self, mono_ns, system_ns):
        with self.lock:
            reference = self.reference
        if reference is None:
            return dict(ready=False, epoch_ns=system_ns, offset_ms=0, age_ms=None,
                        rtt_ms=None, source="system_unverified")
        age_ns = mono_ns - reference["mono_ns"]
        epoch_ns = reference["epoch_ns"] + age_ns
        return dict(ready=0 <= age_ns <= 90_000_000_000, epoch_ns=epoch_ns,
                    offset_ms=(epoch_ns - system_ns)/1e6, age_ms=age_ns/1e6,
                    rtt_ms=reference["rtt_ns"]/1e6, source=reference["server"])

    def close(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=0.1)
