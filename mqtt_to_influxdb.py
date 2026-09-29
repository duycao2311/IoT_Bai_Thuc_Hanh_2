# -*- coding: utf-8 -*-
"""
Script thu thập dữ liệu IoT từ MQTT Broker và ghi vào InfluxDB.
Bài thực hành 2: Thu thập, lưu trữ và tiền xử lý dữ liệu IoT.

Sử dụng:
    python mqtt_to_influxdb.py

Yêu cầu:
    pip install paho-mqtt influxdb-client python-dotenv
"""

import json
import sys
import os
import time
import argparse
from datetime import datetime
from latency_metrics import LatencyRecorder, measure
from latency_clock import EpochClock

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# =============================================================================
# CẤU HÌNH MQTT
# =============================================================================
MQTT_BROKER   = "broker.hivemq.com"
MQTT_PORT     = 1883
MQTT_TOPIC    = "iot/B23DCAT076/esp32/telemetry"
MQTT_CLIENT_ID = "python-collector-B23DCAT076"

# =============================================================================
# CẤU HÌNH INFLUXDB
# =============================================================================
INFLUXDB_URL    = "http://localhost:8086"
INFLUXDB_TOKEN  = "my-super-secret-token"   # Thay bằng token thực tế
INFLUXDB_ORG    = "ptit"
INFLUXDB_BUCKET = "iot_sensor_data"

# =============================================================================
# BIẾN TOÀN CỤC
# =============================================================================
received_count = 0
error_count = 0
influx_write_api = None
latency_recorder = None
sample_target = 0
epoch_clock = None


def validate_sensor_data(data):
    """
    Kiểm tra tính hợp lệ của dữ liệu cảm biến.
    Trả về (is_valid, error_message).
    """
    required_fields = ['temperature', 'humidity', 'light_level']

    for field in required_fields:
        if field not in data:
            return False, f"Thieu truong bat buoc: {field}"

    # Kiểm tra phạm vi hợp lệ
    temp = data.get('temperature', None)
    if temp is not None and (temp < -40 or temp > 80):
        return False, f"Nhiet do ngoai pham vi hop le: {temp}C"

    hum = data.get('humidity', None)
    if hum is not None and (hum < 0 or hum > 100):
        return False, f"Do am ngoai pham vi hop le: {hum}%"

    light = data.get('light_level', None)
    if light is not None and (light < 0 or light > 100):
        return False, f"Anh sang ngoai pham vi hop le: {light}%"

    return True, "OK"


def write_to_influxdb(data):
    """Time a synchronous HTTP write, ending when InfluxDB acknowledges it."""
    global influx_write_api
    start_ns = time.perf_counter_ns()
    try:
        if influx_write_api is None:
            raise RuntimeError("Write API chua duoc khoi tao")
        point = (
            Point("sensor_data")
            .tag("device_id", data.get("device_id", "unknown"))
            .tag("location", "lab_ptit")
            .field("temperature", float(data["temperature"]))
            .field("humidity", float(data["humidity"]))
            .field("light_level", float(data.get("light_level", 0)))
            .field("rssi", int(data.get("rssi", 0)))
            .field("sequence", int(data.get("sequence", 0)))
            .field("uptime_s", int(data.get("uptime_s", 0)))
            .time(datetime.utcnow(), WritePrecision.MS)
        )

        start_ns = time.perf_counter_ns()
        influx_write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        end_ns = time.perf_counter_ns()
        end_wall_ns = time.time_ns()
        return dict(ok=True, start_ns=start_ns, end_ns=end_ns,
                    end_wall_ns=end_wall_ns, error="")

    except Exception as e:
        end_ns = time.perf_counter_ns()
        end_wall_ns = time.time_ns()
        return dict(ok=False, start_ns=start_ns, end_ns=end_ns,
                    end_wall_ns=end_wall_ns, error=str(e))


def on_connect(client, userdata, flags, rc):
    """Callback khi kết nối MQTT thành công."""
    if rc == 0:
        print(f"[MQTT] Ket noi Broker thanh cong: {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Da subscribe topic: {MQTT_TOPIC}")
    else:
        print(f"[MQTT] Ket noi that bai, ma loi: {rc}")


def on_message(client, userdata, msg):
    """Callback khi nhận được bản tin MQTT."""
    received_mono_ns = time.perf_counter_ns()
    received_wall_ns = time.time_ns()
    clock_info = epoch_clock.snapshot(received_mono_ns, received_wall_ns) if epoch_clock else None
    global received_count, error_count

    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)

        received_count += 1
        is_valid, msg_text = validate_sensor_data(data)
        if not is_valid:
            error_count += 1
            print(f"[CANH BAO] Du lieu khong hop le: {msg_text}")
            return

        # Do not print before writing: terminal I/O would inflate the measured path.
        write = write_to_influxdb(data)
        row = measure(data, received_wall_ns, received_mono_ns, write, clock_info)
        timestamp = row["received_utc"]

        print(f"\n{'='*60}")
        print(f"[RECEIVE UTC {timestamp}] Ban tin #{received_count}")
        print(f"  Topic: {msg.topic}")
        print(f"  Nhiet do:  {data.get('temperature', 'N/A')} C")
        print(f"  Do am:     {data.get('humidity', 'N/A')} %RH")
        print(f"  Anh sang:  {data.get('light_level', 'N/A')} %")
        print(f"  RSSI:      {data.get('rssi', 'N/A')} dBm")
        print(f"  Sequence:  {data.get('sequence', 'N/A')}")

        if write["ok"]:
            print(f"  [InfluxDB] Ghi thanh cong!")
        else:
            error_count += 1
            print(f"  [InfluxDB] Ghi that bai: {write['error']}")

        if latency_recorder is not None:
            latency_recorder.record(row)
            if sample_target and latency_recorder.valid_count >= sample_target:
                print(f"[HOAN TAT] Da du {sample_target} mau E2E hop le.")
                client.disconnect()

        # Thống kê
        print(f"  [THONG KE] Tong nhan: {received_count} | Loi: {error_count}")

    except json.JSONDecodeError:
        error_count += 1
        print(f"[LOI] Khong the parse JSON: {msg.payload}")
    except Exception as e:
        error_count += 1
        print(f"[LOI] Xu ly ban tin loi: {e}")


def on_disconnect(client, userdata, rc):
    """Callback khi mất kết nối MQTT."""
    if rc:
        print(f"[MQTT] Mat ket noi (rc={rc}). Dang thu ket noi lai...")


def main():
    global influx_write_api, latency_recorder, sample_target, epoch_clock
    parser = argparse.ArgumentParser(description="Thu thap IoT va do latency thuc te")
    parser.add_argument("--samples", type=int, default=0,
                        help="Tu dung khi du N mau E2E hop le; 0 = chay lien tuc")
    args = parser.parse_args()
    if args.samples < 0:
        parser.error("--samples phai >= 0")
    sample_target = args.samples
    latency_recorder = LatencyRecorder(os.path.join(os.path.dirname(__file__), "latency_results"))

    print("================================================================")
    print("  BAI 2: THU THAP DU LIEU IoT TU MQTT VA LUU VAO INFLUXDB")
    print("================================================================")
    print(f"  MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Topic:       {MQTT_TOPIC}")
    print(f"  InfluxDB:    {INFLUXDB_URL}")
    print(f"  Bucket:      {INFLUXDB_BUCKET}")
    print("================================================================\n")
    print("[DO TRE] MQTT: ESP32 timestamp -> Python callback; InfluxWrite: HTTP write -> ACK.")
    print("[DO TRE] E2E: ESP32 timestamp -> InfluxDB ACK (gom xu ly tai subscriber).")
    print("[CLOCK] Dong bo gio Windows truoc khi do; chay Wokwi 100%, khong pause.")
    print("[CLOCK] MQTT/E2E la uoc luong theo UTC; NTP khong bao dam dong ho trung khop tuyet doi.")
    print(f"[CSV] {latency_recorder.path}")
    print("[EPOCH] ESP32 va Python dung Unix epoch ms UTC, khong cong timezone.")
    print("[EPOCH] Python hieu chinh theo NTP do doc lap; CSV giu ca epoch Windows goc.")
    epoch_clock = EpochClock()
    epoch_clock.start()

    # Khởi tạo InfluxDB client
    influx_client = None
    try:
        influx_client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG
        )
        influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        print("[InfluxDB] Khoi tao client thanh cong.")
    except Exception as e:
        print(f"[InfluxDB] Khong the ket noi: {e}")
        print("[InfluxDB] Tiep tuc thu thap du lieu MQTT nhung khong ghi DB.")

    # Khởi tạo MQTT client
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect

    print(f"[MQTT] Dang ket noi toi {MQTT_BROKER}:{MQTT_PORT}...")

    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        print(f"\n\n[DUNG] Nguoi dung dung chuong trinh.")
        print(f"[THONG KE CUOI] Tong ban tin: {received_count} | Loi: {error_count}")
    except Exception as e:
        print(f"[LOI] Ket noi MQTT that bai: {e}")
    finally:
        mqtt_client.disconnect()
        epoch_clock.close()
        latency_recorder.close()
        if influx_write_api is not None:
            influx_write_api.close()
        if influx_client is not None:
            influx_client.close()
        print("[MQTT] Da ngat ket noi.")


if __name__ == "__main__":
    main()
