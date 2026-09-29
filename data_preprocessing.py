# -*- coding: utf-8 -*-
"""
Script tiền xử lý dữ liệu IoT từ InfluxDB.
Bài thực hành 2: Thu thập, lưu trữ và tiền xử lý dữ liệu IoT.

Chức năng:
    1. Đọc dữ liệu thô từ InfluxDB (hoặc CSV dự phòng)
    2. Xử lý giá trị thiếu (missing values)
    3. Phát hiện và xử lý outlier bằng IQR và Z-score
    4. Resampling theo cửa sổ thời gian
    5. Tạo đặc trưng (rolling mean, delta)
    6. Chuẩn hóa dữ liệu (Min-Max Scaling)
    7. Lưu kết quả vào measurement mới / file CSV

Sử dụng:
    python data_preprocessing.py

Yêu cầu:
    pip install influxdb-client pandas numpy scikit-learn matplotlib
"""

import sys
import os
import warnings
import argparse

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Tùy chọn: import sklearn nếu có
try:
    from sklearn.preprocessing import MinMaxScaler
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    print("[INFO] scikit-learn chua cai dat. Dung MinMax thu cong.")

# Tùy chọn: import InfluxDB nếu có
try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    HAS_INFLUXDB = True
except ImportError:
    HAS_INFLUXDB = False
    print("[INFO] influxdb-client chua cai dat. Doc du lieu tu CSV.")

# =============================================================================
# CẤU HÌNH
# =============================================================================
INFLUXDB_URL    = "http://localhost:8086"
INFLUXDB_TOKEN  = "my-super-secret-token"
INFLUXDB_ORG    = "ptit"
INFLUXDB_BUCKET = "iot_sensor_data"
INFLUXDB_BUCKET_PROCESSED = INFLUXDB_BUCKET

# Đường dẫn file CSV dự phòng
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_CSV    = os.path.join(SCRIPT_DIR, "sample_raw_data.csv")
EXPORTED_CSV = os.path.join(SCRIPT_DIR, "data_explorer_b23dcat076.csv")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "processed_data.csv")


def load_csv(path):
    """Read either an InfluxDB Data Explorer export or a plain sensor CSV."""
    with open(path, encoding="utf-8-sig") as stream:
        is_influx_export = stream.readline().startswith("#group")
    df = pd.read_csv(path, skiprows=3 if is_influx_export else 0)
    time_column = "timestamp" if "timestamp" in df else "_time"
    if time_column not in df:
        raise ValueError(f"CSV thieu cot thoi gian: {path}")
    df[time_column] = pd.to_datetime(df[time_column], utc=True, errors="coerce")
    df = df.dropna(subset=[time_column]).set_index(time_column).sort_index()
    required = ["temperature", "humidity", "light_level", "rssi"]
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"CSV thieu cot cam bien: {sorted(missing)}")
    df[required] = df[required].apply(pd.to_numeric, errors="coerce")
    return df[required + [c for c in ("sequence", "device_id") if c in df]]


def generate_sample_data(n_points=200):
    """
    Tạo dữ liệu mẫu mô phỏng cảm biến IoT (dùng khi chưa có InfluxDB).
    Thêm noise, missing values và outlier để minh họa tiền xử lý.
    """
    print("[SAMPLE] Tao du lieu mau mo phong...")

    np.random.seed(42)
    timestamps = pd.date_range(
        start=datetime.now() - timedelta(hours=2),
        periods=n_points,
        freq='30s'
    )

    # Dữ liệu nhiệt độ: 25C +/- nhiễu + vài outlier
    temp = 25.0 + np.random.normal(0, 1.5, n_points)
    # Thêm outlier
    outlier_idx = np.random.choice(n_points, 5, replace=False)
    temp[outlier_idx] = np.random.choice([60, -20, 100, -10, 55], 5)

    # Dữ liệu độ ẩm: 55% +/- nhiễu
    hum = 55.0 + np.random.normal(0, 5, n_points)
    hum_outlier_idx = np.random.choice(n_points, 3, replace=False)
    hum[hum_outlier_idx] = np.random.choice([120, -5, 150], 3)

    # Dữ liệu ánh sáng: biến đổi theo thời gian (mô phỏng ngày/đêm)
    hours = np.array([(t.hour + t.minute / 60.0) for t in timestamps])
    light = 50 + 30 * np.sin((hours - 6) * np.pi / 12) + np.random.normal(0, 5, n_points)
    light = np.clip(light, 0, 100)

    # RSSI
    rssi = -50 + np.random.normal(0, 5, n_points)

    # Sequence tăng dần
    sequence = np.arange(1, n_points + 1)

    df = pd.DataFrame({
        'timestamp': timestamps,
        'temperature': temp,
        'humidity': hum,
        'light_level': light,
        'rssi': rssi,
        'sequence': sequence,
        'device_id': 'ESP32-BAI2-B23DCAT076'
    })

    # Thêm missing values
    missing_idx = np.random.choice(n_points, 8, replace=False)
    for idx in missing_idx[:4]:
        df.loc[idx, 'temperature'] = np.nan
    for idx in missing_idx[4:]:
        df.loc[idx, 'humidity'] = np.nan

    df.set_index('timestamp', inplace=True)
    df.to_csv(RAW_CSV)
    print(f"[SAMPLE] Da luu {n_points} ban ghi vao: {RAW_CSV}")

    return df


def load_data_from_influxdb():
    """Đọc dữ liệu từ InfluxDB."""
    if not HAS_INFLUXDB:
        return None

    try:
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        query_api = client.query_api()

        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -2h)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        result = query_api.query_data_frame(query)
        if result is not None and len(result) > 0:
            result.set_index('_time', inplace=True)
            print(f"[InfluxDB] Doc duoc {len(result)} ban ghi.")
            return result

    except Exception as e:
        print(f"[InfluxDB] Khong the doc du lieu: {e}")

    return None


def load_data(input_csv=None):
    """Tải dữ liệu: thử InfluxDB trước, nếu không thì CSV, cuối cùng tạo mẫu."""
    if input_csv:
        print(f"[CSV] Doc du lieu thuc tu: {input_csv}")
        return load_csv(input_csv)
    # Thử InfluxDB
    df = load_data_from_influxdb()
    if df is not None and len(df) > 10:
        return df

    # Prefer the real Data Explorer export over the older generated example.
    if os.path.exists(EXPORTED_CSV):
        print(f"[CSV] Doc du lieu thuc tu: {EXPORTED_CSV}")
        return load_csv(EXPORTED_CSV)

    # Thử CSV
    if os.path.exists(RAW_CSV):
        print(f"[CSV] Doc du lieu tu: {RAW_CSV}")
        return load_csv(RAW_CSV)

    # Tạo dữ liệu mẫu
    return generate_sample_data()


def step1_handle_missing_values(df):
    """Bước 1: Xử lý giá trị thiếu (Missing Values)."""
    print("\n" + "=" * 60)
    print("BUOC 1: XU LY GIA TRI THIEU (MISSING VALUES)")
    print("=" * 60)

    numeric_cols = ['temperature', 'humidity', 'light_level', 'rssi']
    missing_before = df[numeric_cols].isnull().sum()
    print(f"\nSo gia tri thieu TRUOC xu ly:")
    for col in numeric_cols:
        count = missing_before.get(col, 0)
        if count > 0:
            print(f"  {col}: {count} gia tri thieu")

    # Phương pháp: Nội suy tuyến tính (Linear Interpolation)
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].interpolate(method='linear', limit_direction='both')

    missing_after = df[numeric_cols].isnull().sum()
    print(f"\nSo gia tri thieu SAU xu ly (noi suy tuyen tinh):")
    for col in numeric_cols:
        count = missing_after.get(col, 0)
        print(f"  {col}: {count} gia tri thieu")

    return df


def step2_detect_outliers_iqr(df):
    """Bước 2a: Phát hiện và xử lý outlier bằng phương pháp IQR."""
    print("\n" + "=" * 60)
    print("BUOC 2a: PHAT HIEN OUTLIER BANG IQR")
    print("=" * 60)

    sensor_cols = ['temperature', 'humidity', 'light_level']
    outlier_info = {}

    for col in sensor_cols:
        if col not in df.columns:
            continue

        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        outliers = df[(df[col] < lower) | (df[col] > upper)]
        n_outliers = len(outliers)
        outlier_info[col] = {
            'count': n_outliers,
            'Q1': Q1, 'Q3': Q3, 'IQR': IQR,
            'lower': lower, 'upper': upper
        }

        print(f"\n  {col}:")
        print(f"    Q1={Q1:.2f}, Q3={Q3:.2f}, IQR={IQR:.2f}")
        print(f"    Gioi han: [{lower:.2f}, {upper:.2f}]")
        print(f"    So outlier: {n_outliers}")

        # Thay thế outlier bằng giá trị biên (capping/clipping)
        df[col] = df[col].clip(lower=lower, upper=upper)

    return df, outlier_info


def step2b_detect_outliers_zscore(df):
    """Bước 2b: Phát hiện outlier bằng Z-score (để so sánh)."""
    print("\n" + "=" * 60)
    print("BUOC 2b: PHAT HIEN OUTLIER BANG Z-SCORE")
    print("=" * 60)

    sensor_cols = ['temperature', 'humidity', 'light_level']
    zscore_threshold = 3.0

    for col in sensor_cols:
        if col not in df.columns:
            continue

        mean = df[col].mean()
        std = df[col].std()

        if std == 0:
            print(f"  {col}: Do lech chuan = 0, bo qua.")
            continue

        z_scores = np.abs((df[col] - mean) / std)
        n_outliers = (z_scores > zscore_threshold).sum()

        print(f"\n  {col}:")
        print(f"    Mean={mean:.2f}, Std={std:.2f}")
        print(f"    Nguong Z-score: {zscore_threshold}")
        print(f"    So outlier (Z > {zscore_threshold}): {n_outliers}")

    return df


def step3_resampling(df):
    """Bước 3: Resampling theo cửa sổ thời gian."""
    print("\n" + "=" * 60)
    print("BUOC 3: RESAMPLING THEO CUA SO THOI GIAN")
    print("=" * 60)

    numeric_cols = ['temperature', 'humidity', 'light_level', 'rssi']

    # Resampling mỗi 1 phút (lấy giá trị trung bình)
    df_resampled = df[numeric_cols].resample('1min').agg({
        'temperature': 'mean',
        'humidity': 'mean',
        'light_level': 'mean',
        'rssi': 'mean'
    }).dropna()

    print(f"  So ban ghi GOC:      {len(df)}")
    print(f"  So ban ghi SAU resample (1 phut): {len(df_resampled)}")
    print(f"  Phuong phap:         Trung binh (mean) moi 1 phut")

    return df_resampled


def step4_feature_engineering(df):
    """Bước 4: Tạo đặc trưng đơn giản (Rolling Mean, Delta)."""
    print("\n" + "=" * 60)
    print("BUOC 4: TAO DAC TRUNG (FEATURE ENGINEERING)")
    print("=" * 60)

    # Rolling Mean (cửa sổ 5 mẫu)
    for col in ['temperature', 'humidity', 'light_level']:
        if col in df.columns:
            df[f'{col}_rolling_mean_5'] = df[col].rolling(window=5, min_periods=1).mean()
            print(f"  Tao: {col}_rolling_mean_5 (trung binh truot 5 mau)")

    # Delta (chênh lệch giữa các mẫu liên tiếp)
    for col in ['temperature', 'humidity']:
        if col in df.columns:
            df[f'{col}_delta'] = df[col].diff()
            print(f"  Tao: {col}_delta (do thay doi giua cac mau)")

    # Nhiệt độ cảm nhận (Heat Index đơn giản)
    if 'temperature' in df.columns and 'humidity' in df.columns:
        df['heat_index'] = df['temperature'] + 0.33 * (df['humidity'] / 100 * 6.105 * np.exp(17.27 * df['temperature'] / (237.7 + df['temperature']))) - 4.0
        print(f"  Tao: heat_index (chi so nhiet cam nhan)")

    return df


def step5_normalize(df):
    """Bước 5: Chuẩn hóa dữ liệu (Min-Max Scaling)."""
    print("\n" + "=" * 60)
    print("BUOC 5: CHUAN HOA DU LIEU (MIN-MAX SCALING)")
    print("=" * 60)

    cols_to_normalize = ['temperature', 'humidity', 'light_level']
    existing_cols = [c for c in cols_to_normalize if c in df.columns]

    if HAS_SKLEARN:
        scaler = MinMaxScaler()
        df_normalized = df.copy()
        for col in existing_cols:
            values = df[[col]].dropna()
            if len(values) > 0:
                scaled = scaler.fit_transform(values)
                df_normalized.loc[values.index, f'{col}_normalized'] = scaled.flatten()
                print(f"  {col}: Min={df[col].min():.2f}, Max={df[col].max():.2f} -> [0, 1]")
    else:
        df_normalized = df.copy()
        for col in existing_cols:
            col_min = df[col].min()
            col_max = df[col].max()
            if col_max > col_min:
                df_normalized[f'{col}_normalized'] = (df[col] - col_min) / (col_max - col_min)
                print(f"  {col}: Min={col_min:.2f}, Max={col_max:.2f} -> [0, 1]")

    return df_normalized


def save_results(df, write_influx=True):
    """Lưu kết quả tiền xử lý."""
    print("\n" + "=" * 60)
    print("LUU KET QUA")
    print("=" * 60)

    # Lưu CSV
    df.to_csv(OUTPUT_CSV)
    print(f"  Da luu file CSV: {OUTPUT_CSV}")
    print(f"  So ban ghi: {len(df)}")
    print(f"  So cot: {len(df.columns)}")
    print(f"  Cac cot: {', '.join(df.columns.tolist())}")

    # Thử ghi vào InfluxDB bucket mới
    if HAS_INFLUXDB and write_influx:
        try:
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            write_api = client.write_api(write_options=SYNCHRONOUS)

            for idx, row in df.iterrows():
                point = Point("processed_sensor_data")
                point.tag("device_id", "ESP32-BAI2-B23DCAT076")
                point.tag("processing", "v1")

                for col in df.columns:
                    if pd.notna(row[col]):
                        point.field(col, float(row[col]))

                point.time(idx, WritePrecision.MS)
                write_api.write(bucket=INFLUXDB_BUCKET_PROCESSED, org=INFLUXDB_ORG, record=point)

            print(f"  [InfluxDB] Da ghi {len(df)} ban ghi vao bucket: {INFLUXDB_BUCKET_PROCESSED}")

        except Exception as e:
            print(f"  [InfluxDB] Khong the ghi: {e}")


def print_summary(df_raw, df_processed):
    """In bảng tóm tắt kết quả tiền xử lý."""
    print("\n" + "=" * 60)
    print("BANG TOM TAT KET QUA TIEN XU LY")
    print("=" * 60)

    cols = ['temperature', 'humidity', 'light_level']

    print(f"\n{'Thuoc tinh':<15} {'Min':>8} {'Max':>8} {'Mean':>8} {'Std':>8}")
    print("-" * 55)
    for col in cols:
        if col in df_processed.columns:
            print(f"  {col:<13} {df_processed[col].min():>8.2f} "
                  f"{df_processed[col].max():>8.2f} "
                  f"{df_processed[col].mean():>8.2f} "
                  f"{df_processed[col].std():>8.2f}")


def main():
    parser = argparse.ArgumentParser(description="Tien xu ly du lieu cam bien IoT")
    parser.add_argument("--input-csv", help="CSV du lieu thuc xuat tu InfluxDB")
    parser.add_argument("--csv-only", action="store_true", help="Chi ghi processed_data.csv")
    args = parser.parse_args()
    print("================================================================")
    print("  BAI 2: TIEN XU LY DU LIEU IoT")
    print("================================================================\n")

    # Tải dữ liệu
    df_raw = load_data(args.input_csv)
    print(f"\n[DU LIEU THO] {len(df_raw)} ban ghi, {len(df_raw.columns)} cot")

    # Pipeline tiền xử lý
    df = df_raw.copy()
    df = step1_handle_missing_values(df)
    df, outlier_info = step2_detect_outliers_iqr(df)
    df = step2b_detect_outliers_zscore(df)
    df_resampled = step3_resampling(df)
    df_features = step4_feature_engineering(df_resampled)
    df_final = step5_normalize(df_features)

    # Lưu kết quả
    save_results(df_final, write_influx=not args.csv_only)
    print_summary(df_raw, df_final)

    print("\n================================================================")
    print("  HOAN TAT TIEN XU LY DU LIEU!")
    print("================================================================")


if __name__ == "__main__":
    main()
