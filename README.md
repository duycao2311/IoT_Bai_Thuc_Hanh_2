# Bài thực hành 2 IoT — B23DCAT076

Sinh viên: **Phạm Cao Duy**. Pipeline thực hiện:

```text
ESP32/Wokwi → MQTT (broker.hivemq.com) → Python subscriber → InfluxDB
                                                          ↓ xuất CSV
                                     tiền xử lý → CSV → Streamlit
```

Dashboard hiện đọc CSV offline. Thu thập MQTT và ghi InfluxDB chạy trực tuyến; chưa có truy vấn liên tục từ dashboard đến database.

## 1. Chuẩn bị

Cần Python 3.11 trở lên; Docker Desktop để chạy database; VS Code với PlatformIO và Wokwi để mô phỏng firmware. Wokwi for VS Code cần quyền sử dụng phù hợp. Mở PowerShell tại thư mục repository sau khi clone/tải xuống:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Các lệnh dưới chạy tại thư mục này, không cần kích hoạt virtual environment.

## 2. Demo nhanh bằng dữ liệu đã có

```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```

Mở địa chỉ terminal hiển thị (thường http://localhost:8501). Chọn **Du lieu CSV (Offline)** và bỏ chọn tự động làm mới khi xem bộ dữ liệu cố định. Tab **So sanh tho / da xu ly** so sánh hai CSV đi kèm. Không cần Docker/Wokwi cho chế độ này.

- `data_explorer_b23dcat076.csv`: 240 bản ghi thực thu từ Wokwi qua MQTT/InfluxDB, sequence 30–269; 29/09/2026 06:43:50–07:03:48 UTC.
- `processed_data.csv`: 21 cửa sổ một phút từ bộ dữ liệu trên, có đặc trưng và chuẩn hóa.
- `sample_raw_data.csv`: 200 bản ghi sinh để thử chức năng, không dùng làm kết quả thực nghiệm trong báo cáo.

"Dữ liệu thực" ở đây là dữ liệu thu từ phiên mô phỏng Wokwi, không phải đo ngoài hiện trường bằng thiết bị vật lý.

## 3. Chạy lại pipeline thu thập

### Database

```powershell
docker compose up -d influxdb
```

Mở http://localhost:8086. Cấu hình lab đồng nhất trong Compose và các script: tài khoản `admin`, mật khẩu `admin12345`, organization `ptit`, bucket `iot_sensor_data`, token `my-super-secret-token`. Đây là thông tin mẫu cho môi trường cục bộ; phải thay đồng bộ nếu triển khai công khai. Khởi tạo này chỉ áp dụng cho volume mới; volume cũ giữ cấu hình đã có.

Compose còn có Mosquitto và Grafana, nhưng pipeline mặc định dùng HiveMQ công cộng và Streamlit nên không cần khởi động hai dịch vụ đó. Không đổi broker sang localhost khi chạy Wokwi nếu chưa cấu hình gateway phù hợp.

### Firmware và subscriber

```powershell
.\.venv\Scripts\python.exe -m pip install platformio
.\.venv\Scripts\python.exe -m platformio run
.\.venv\Scripts\python.exe mqtt_to_influxdb.py
```

Chờ Python báo kết nối MQTT; trong VS Code mở `diagram.json` và chạy **Wokwi: Start Simulator**. `wokwi.toml` trỏ tới firmware do PlatformIO build trong `.pio`; repository không kèm binary. Có thể dùng `sketch.ino`, `diagram.json` và các thư viện trong `libraries.txt` cho Wokwi online.

Topic telemetry: `iot/B23DCAT076/esp32/telemetry`; thiết bị `ESP32-BAI2-B23DCAT076`; chu kỳ danh định 5 giây. Cần truy cập Internet đến MQTT TCP 1883 và NTP UDP 123. Firmware chờ NTP hợp lệ trước khi publish telemetry. Chỉ chạy một subscriber cho một phiên đo để tránh ghi lặp.

Schema: measurement `sensor_data`; tags `device_id`, `location`; fields `temperature`, `humidity`, `light_level`, `rssi`, `sequence`, `uptime_s`; timestamp UTC phía Python. Retention cấu hình 7 ngày. Sequence hỗ trợ đối chiếu; code chưa có hàng đợi bền vững hay khử trùng theo sequence.

### Xuất dữ liệu thật

Trong Data Explorer chọn Script Editor, chạy Flux sau cho khoảng thời gian vừa thu:

```flux
from(bucket: "iot_sensor_data")
  |> range(start: -2h)
  |> filter(fn: (r) => r._measurement == "sensor_data")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {_time: "timestamp"})
  |> keep(columns: ["timestamp", "device_id", "location", "humidity", "light_level", "rssi", "sequence", "temperature", "uptime_s"])
```

Tải CSV và lưu tên `data_explorer_b23dcat076.csv` cạnh script. Giữ định dạng annotated CSV với 3 dòng metadata trước dòng tiêu đề vì dashboard hiện đọc theo định dạng này. Sao lưu CSV demo trước nếu muốn giữ phiên gốc. Dữ liệu lịch sử đi kèm có thể đã hết retention trong database, nhưng CSV vẫn dùng độc lập được.

## 4. Tiền xử lý

Chạy lại trên CSV thực, chỉ ghi file kết quả:

```powershell
.\.venv\Scripts\python.exe data_preprocessing.py --input-csv data_explorer_b23dcat076.csv --csv-only
```

Chương trình nội suy missing, giới hạn outlier theo IQR, in Z-score tham khảo, resample 1 phút, tạo rolling mean/delta/heat index và Min–Max. Đầu ra `processed_data.csv` được ghi đè. Muốn ghi thêm vào measurement `processed_sensor_data` trong bucket `iot_sensor_data`, bỏ `--csv-only` khi InfluxDB đang chạy. Tùy chọn `--input-csv` giúp chọn chắc chắn dữ liệu thực, tránh rơi về dữ liệu mẫu khi database không truy cập được.

## 5. Độ trễ và minh chứng

Xem [HUONG_DAN_DO_DO_TRE.md](HUONG_DAN_DO_DO_TRE.md). Một phiên mới có thể chạy:

```powershell
.\.venv\Scripts\python.exe mqtt_to_influxdb.py --samples 100
```

CSV đi kèm `latency_results/latency_20260929T103433_336384Z.csv` có **13 mẫu**, không phải 100. File `_summary.csv` là thống kê phiên đó. Báo cáo chỉ phân tích một mẫu **sequence 98** khớp ảnh: MQTT **306.002 ms**, InfluxWrite **48.530 ms**, E2E **354.588 ms**, trạng thái `ok`. Đây không phải trung bình toàn hệ thống. Đồng hồ được đối chiếu NTP; phép đo liên thiết bị vẫn chịu sai số đồng bộ.

## 6. File trong repository

- `src/main.cpp`, `sketch.ino`: firmware hai môi trường.
- `platformio.ini`, `wokwi.toml`, `diagram.json`, `libraries.txt`: build và mô phỏng.
- `mqtt_to_influxdb.py`: nhận, validate và ghi InfluxDB.
- `latency_clock.py`, `latency_metrics.py`: đồng hồ NTP, đo và xuất CSV.
- `data_preprocessing.py`, `dashboard.py`: xử lý và hiển thị.
- `docker-compose.yml`, `config/mosquitto.conf`, `requirements.txt`: cấu hình/phụ thuộc.
- `tests/test_latency.py`: kiểm tra logic tính độ trễ.
- `docs/`: báo cáo hoàn thiện và ảnh minh chứng được dùng.

Chạy kiểm tra logic: `.\.venv\Scripts\python.exe -m unittest discover -s tests -v`.

## 7. Giới hạn và hướng phát triển

Chưa đo tải nhiều thiết bị, dung lượng đĩa hoặc độ ổn định dài hạn. Khi ghi thất bại chưa có retry bền vững; dashboard CSV chưa giám sát database trực tiếp. Có thể bổ sung truy vấn live, queue/retry, session ID để khử trùng, TLS và thử tải. Các nội dung này là đề xuất, chưa phải chức năng đã hoàn thành.
