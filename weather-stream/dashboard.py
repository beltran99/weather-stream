import streamlit as st
import pandas as pd
import plotly.express as px
import json
import time
import threading
import queue
from kafka import KafkaConsumer

# Automatically rerun every N seconds
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="🌦️ Real-Time Weather Dashboard", layout="wide")

TOPIC = "weather-data"
BOOTSTRAP = "localhost:9092"
MAX_ROWS = 100
REFRESH_INTERVAL_MS = 30000  # 30 seconds

# Trigger periodic reruns
st_autorefresh(interval=REFRESH_INTERVAL_MS, key="datarefresh")

# --- Background Consumer Thread (non-blocking) ---
@st.cache_resource
def get_consumer_queue():
    """
    Start a single background Kafka consumer thread.
    It runs once per session and fills a queue for Streamlit to read.
    """
    q = queue.Queue()

    def consume_forever():
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="streamlit-dashboard",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            for msg in consumer:
                q.put(msg)
        except Exception as e:
            q.put({"_error": str(e)})

    t = threading.Thread(target=consume_forever, daemon=True)
    t.start()
    return q

msg_queue = get_consumer_queue()

# --- Session state for data persistence ---
if "data" not in st.session_state:
    st.session_state.data = pd.DataFrame(columns=["date", "temperature_2m", "relative_humidity_2m", "precipitation_probability"])

# --- Drain Queue (non-blocking) ---
new_forecast = None
while True:
    try:
        msg = msg_queue.get_nowait()
    except queue.Empty:
        break

    if isinstance(msg, dict) and msg.get("_error"):
        st.error(f"Kafka error: {msg['_error']}")
        continue

    key = msg.key.decode("utf-8")
    weather_df = pd.DataFrame(msg.value)
    weather_df["date"] = pd.to_datetime(weather_df["date"])
    new_forecast = weather_df[["date", "temperature_2m", "relative_humidity_2m", "precipitation_probability"]]

if new_forecast is not None:
    st.session_state.data = pd.concat([st.session_state.data, new_forecast]).tail(MAX_ROWS)

df = st.session_state.data

# --- UI ---
st.title("🌦️ Real-Time Weather Forecast")
st.caption(f"Auto-updating every {REFRESH_INTERVAL_MS/1000:.0f}s from Kafka topic: '{TOPIC}'")
temp_placeholder = st.empty()
humidity_placeholder = st.empty()
precipitation_placeholder = st.empty()

if df.empty:
    st.info("Waiting for Kafka messages...")
    st.stop()

st.success(f"Received {len(df)} records (last: {df['date'].iloc[-1]})")

# --- Draw Temperature Chart ---
with temp_placeholder.container():
    fig_temp = px.line(
        st.session_state.data,
        x="date",
        y="temperature_2m",
        title="🌡️ Temperature (°C) Over Time",
        markers=True,
    )
    fig_temp.update_layout({
        "title": {
            "font" : {"size": 40}
        },
        "xaxis": {
            "title": {
                "text" : "Date",
                "font" : {"size": 24}
            },
            "tickfont": {"size": 24}
        },
        "yaxis": {
            "title": {
                "text" : "Temperature [°C]",
                "font" : {"size": 24}
            },
            "tickfont": {"size": 24}
        },
    })
    fig_temp_key = str(time.time()) + "_temp_chart"
    temp_placeholder.plotly_chart(fig_temp, use_container_width=True, key=fig_temp_key)

# --- Draw Humidity Chart ---
with humidity_placeholder.container():
    fig_humidity = px.line(
        st.session_state.data,
        x="date",
        y="relative_humidity_2m",
        title="💧 Humidity (%) Over Time",
        markers=True,
    )
    fig_humidity.update_layout({
        "title": {
            "font" : {"size": 40}
        },
        "xaxis": {
            "title": {
                "text" : "Date",
                "font" : {"size": 24}
            },
            "tickfont": {"size": 24}
        },
        "yaxis": {
            "title": {
                "text" : "Relative Humidity [%]",
                "font" : {"size": 24}
            },
            "tickfont": {"size": 24}
        },
    })
    fig_humidity_key = str(time.time()) + "_humidity_chart"
    humidity_placeholder.plotly_chart(fig_humidity, use_container_width=True, key=fig_humidity_key)
    
# --- Draw Precipitation Chart ---
with precipitation_placeholder.container():
    fig_precipitation = px.line(
        st.session_state.data,
        x="date",
        y="precipitation_probability",
        title="🌧️ Precipitation Probability (%) Over Time",
        markers=True,
    )
    fig_precipitation.update_layout({
        "title": {
            "font" : {"size": 40}
        },
        "xaxis": {
            "title": {
                "text" : "Date",
                "font" : {"size": 24}
            },
            "tickfont": {"size": 24}
        },
        "yaxis": {
            "title": {
                "text" : "Precipitation probability [%]",
                "font" : {"size": 24}
            },
            "tickfont": {"size": 24}
        },
    })
    fig_precipitation_key = str(time.time()) + "_precipitation_chart"
    precipitation_placeholder.plotly_chart(fig_precipitation, use_container_width=True, key=fig_precipitation_key)
