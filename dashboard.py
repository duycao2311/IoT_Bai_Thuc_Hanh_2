# -*- coding: utf-8 -*-
"""
Dashboard giám sát dữ liệu IoT real-time bằng Streamlit.
Bài thực hành 2: Thu thập, lưu trữ và tiền xử lý dữ liệu IoT.

Sử dụng:
    streamlit run dashboard.py

Yêu cầu:
    pip install streamlit pandas numpy plotly paho-mqtt
"""

import sys
import os
import json
import time
import threading
from datetime import datetime, timedelta
from collections import deque

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Tùy chọn: import MQTT
try:
    import paho.mqtt.client as mqtt
    HAS_MQTT = True
except ImportError:
    HAS_MQTT = False

# =============================================================================
# CẤU HÌNH
# =============================================================================
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT   = 1883
MQTT_TOPIC  = "iot/B23DCAT076/esp32/telemetry"

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
RAW_CSV      = os.path.join(SCRIPT_DIR, "data_explorer_b23dcat076.csv")
SAMPLE_CSV   = os.path.join(SCRIPT_DIR, "sample_raw_data.csv")
PROCESSED_CSV = os.path.join(SCRIPT_DIR, "processed_data.csv")

MAX_REALTIME_POINTS = 100


# =============================================================================
# STREAMLIT PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="IoT Sensor Dashboard - Bai 2",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


def load_csv_data():
    """Tải dữ liệu từ file CSV."""
    raw_df = None
    processed_df = None

    if os.path.exists(RAW_CSV):
        raw_df = pd.read_csv(RAW_CSV, skiprows=3, index_col='timestamp', parse_dates=True)
        raw_df = raw_df.drop(columns=['Unnamed: 0', 'result', 'table'], errors='ignore')
    elif os.path.exists(SAMPLE_CSV):
        raw_df = pd.read_csv(SAMPLE_CSV, index_col='timestamp', parse_dates=True)

    if os.path.exists(PROCESSED_CSV):
        processed_df = pd.read_csv(PROCESSED_CSV, index_col=0, parse_dates=True)

    return raw_df, processed_df


def create_gauge(value, title, min_val, max_val, unit, color_ranges=None):
    """Tạo gauge chart cho giá trị cảm biến."""
    if color_ranges is None:
        color_ranges = [
            [0, 0.33, "green"],
            [0.33, 0.66, "yellow"],
            [0.66, 1.0, "red"]
        ]

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        title={'text': title, 'font': {'size': 16}},
        number={'suffix': f' {unit}', 'font': {'size': 24}},
        gauge={
            'axis': {'range': [min_val, max_val], 'tickwidth': 1},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [min_val + (max_val - min_val) * r[0],
                           min_val + (max_val - min_val) * r[1]],
                 'color': r[2]}
                for r in color_ranges
            ],
        }
    ))
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
    return fig


def create_timeseries_chart(df, columns, title, y_label):
    """Tạo biểu đồ time-series."""
    fig = go.Figure()

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    for i, col in enumerate(columns):
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df.index,
                y=df[col],
                mode='lines',
                name=col,
                line=dict(color=colors[i % len(colors)], width=2)
            ))

    fig.update_layout(
        title=title,
        xaxis_title="Thoi gian",
        yaxis_title=y_label,
        height=400,
        template="plotly_dark",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig


def create_comparison_chart(raw_df, processed_df, column):
    """Tạo biểu đồ so sánh dữ liệu thô và đã xử lý."""
    fig = make_subplots(rows=2, cols=1,
                        subplot_titles=[f'{column} - Du lieu tho',
                                        f'{column} - Du lieu da xu ly'],
                        vertical_spacing=0.15)

    if raw_df is not None and column in raw_df.columns:
        fig.add_trace(go.Scatter(
            x=raw_df.index, y=raw_df[column],
            mode='lines+markers', name='Tho',
            marker=dict(size=3), line=dict(color='#ff6b6b', width=1)
        ), row=1, col=1)

    if processed_df is not None and column in processed_df.columns:
        fig.add_trace(go.Scatter(
            x=processed_df.index, y=processed_df[column],
            mode='lines', name='Da xu ly',
            line=dict(color='#51cf66', width=2)
        ), row=2, col=1)

    fig.update_layout(height=500, template="plotly_dark")
    return fig


def main():
    # Sidebar
    st.sidebar.title("⚙️ Cau hinh Dashboard")
    st.sidebar.markdown("---")

    data_source = st.sidebar.radio(
        "Nguon du lieu:",
        ["Du lieu CSV (Offline)", "Du lieu mau (Demo)"]
    )

    auto_refresh = st.sidebar.checkbox("Tu dong lam moi (5s)", value=False)
    if auto_refresh:
        time.sleep(5)
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.info(
        "**Bai thuc hanh 2**\n\n"
        "Thu thap, luu tru va tien xu ly du lieu IoT\n\n"
        "SV: Pham Cao Duy\n\n"
        "MSV: B23DCAT076"
    )

    # Header
    st.title("📊 IoT Sensor Monitoring Dashboard")
    st.markdown("**Bai thuc hanh 2: Thu thap, luu tru va tien xu ly du lieu IoT**")
    st.markdown("---")

    # Tải dữ liệu
    raw_df, processed_df = load_csv_data()

    if data_source == "Du lieu mau (Demo)" or (raw_df is None and processed_df is None):
        # Tạo dữ liệu demo
        np.random.seed(42)
        n = 120
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(minutes=60),
            periods=n, freq='30s'
        )
        raw_df = pd.DataFrame({
            'temperature': 25 + np.random.normal(0, 2, n),
            'humidity': 55 + np.random.normal(0, 5, n),
            'light_level': 50 + 20 * np.sin(np.linspace(0, 4 * np.pi, n)) + np.random.normal(0, 3, n),
            'rssi': -50 + np.random.normal(0, 3, n),
            'sequence': range(1, n + 1)
        }, index=timestamps)

        processed_df = raw_df.copy()
        # Đơn giản: smooth
        for col in ['temperature', 'humidity', 'light_level']:
            processed_df[col] = processed_df[col].rolling(5, min_periods=1).mean()
            processed_df[f'{col}_rolling_mean_5'] = processed_df[col].rolling(5, min_periods=1).mean()

    # === ROW 1: Giá trị hiện tại ===
    st.subheader("🔴 Gia tri hien tai (Latest)")
    col1, col2, col3, col4 = st.columns(4)

    if raw_df is not None and len(raw_df) > 0:
        latest = raw_df.iloc[-1]

        with col1:
            temp_val = latest.get('temperature', 25.0)
            st.metric(
                label="🌡️ Nhiet do",
                value=f"{temp_val:.1f} °C",
                delta=f"{raw_df['temperature'].diff().iloc[-1]:.1f} °C" if len(raw_df) > 1 else None
            )
            st.plotly_chart(
                create_gauge(temp_val, "Nhiet do", 0, 50, "°C",
                             [[0, 0.36, "#51cf66"], [0.36, 0.72, "#fcc419"], [0.72, 1.0, "#ff6b6b"]]),
                width="stretch"
            )

        with col2:
            hum_val = latest.get('humidity', 55.0)
            st.metric(
                label="💧 Do am",
                value=f"{hum_val:.1f} %RH",
                delta=f"{raw_df['humidity'].diff().iloc[-1]:.1f} %" if len(raw_df) > 1 else None
            )
            st.plotly_chart(
                create_gauge(hum_val, "Do am", 0, 100, "%RH",
                             [[0, 0.3, "#ff6b6b"], [0.3, 0.7, "#51cf66"], [0.7, 1.0, "#339af0"]]),
                width="stretch"
            )

        with col3:
            light_val = latest.get('light_level', 50.0)
            st.metric(
                label="☀️ Anh sang",
                value=f"{light_val:.1f} %",
                delta=f"{raw_df['light_level'].diff().iloc[-1]:.1f} %" if len(raw_df) > 1 else None
            )
            st.plotly_chart(
                create_gauge(light_val, "Anh sang", 0, 100, "%",
                             [[0, 0.2, "#495057"], [0.2, 0.6, "#fcc419"], [0.6, 1.0, "#ff922b"]]),
                width="stretch"
            )

        with col4:
            rssi_val = latest.get('rssi', -50.0)
            seq_val = latest.get('sequence', 0)
            st.metric(label="📶 RSSI", value=f"{rssi_val:.0f} dBm")
            st.metric(label="📦 Sequence", value=f"#{int(seq_val)}")
            st.metric(label="📊 Tong ban ghi", value=f"{len(raw_df)}")

    st.markdown("---")

    # === ROW 2: Biểu đồ time-series ===
    st.subheader("📈 Bieu do du lieu theo thoi gian")

    tab1, tab2, tab3 = st.tabs(["Nhiet do & Do am", "Anh sang", "So sanh tho / da xu ly"])

    with tab1:
        if raw_df is not None:
            st.plotly_chart(
                create_timeseries_chart(raw_df, ['temperature', 'humidity'],
                                         "Nhiet do va Do am theo thoi gian",
                                         "Gia tri"),
                width="stretch"
            )

    with tab2:
        if raw_df is not None:
            st.plotly_chart(
                create_timeseries_chart(raw_df, ['light_level'],
                                         "Cuong do anh sang theo thoi gian",
                                         "Anh sang (%)"),
                width="stretch"
            )

    with tab3:
        compare_col = st.selectbox(
            "Chon thuoc tinh so sanh:",
            ['temperature', 'humidity', 'light_level']
        )
        st.plotly_chart(
            create_comparison_chart(raw_df, processed_df, compare_col),
            width="stretch"
        )

    st.markdown("---")

    # === ROW 3: Thống kê ===
    st.subheader("📋 Thong ke du lieu")
    col_stat1, col_stat2 = st.columns(2)

    with col_stat1:
        st.markdown("**Du lieu tho:**")
        if raw_df is not None:
            stats = raw_df[['temperature', 'humidity', 'light_level']].describe()
            st.dataframe(stats.round(2), width="stretch")

    with col_stat2:
        st.markdown("**Du lieu da xu ly:**")
        if processed_df is not None:
            cols_to_show = [c for c in ['temperature', 'humidity', 'light_level'] if c in processed_df.columns]
            if cols_to_show:
                stats = processed_df[cols_to_show].describe()
                st.dataframe(stats.round(2), width="stretch")

    st.markdown("---")

    # === ROW 4: Dữ liệu gần nhất ===
    st.subheader("📄 Du lieu gan nhat")
    if raw_df is not None:
        st.dataframe(raw_df.tail(20), width="stretch")

    # Footer
    st.markdown("---")
    st.markdown(
        "<p style='text-align: center; color: gray;'>"
        "IoT Sensor Dashboard | Bai thuc hanh 2 | "
        "Pham Cao Duy - B23DCAT076 | IoT va Ung dung (INT14149)"
        "</p>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()

