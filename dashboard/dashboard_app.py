import streamlit as st
import pandas as pd
import os

st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide"
)

@st.cache_data
def load_data():
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_path, "data_lake", "curated", "transformed_data.parquet")
    return pd.read_parquet(path)

with st.spinner("جاري تحميل البيانات..."):
    df = load_data()

# Header
st.title("🚕 NYC Taxi Trips — Analytics Dashboard")
st.markdown(f"### تحليل {len(df):,} رحلة حقيقية — يناير 2023")
st.divider()

# KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric(" متوسط السعر", f"${df['total_amount'].mean():.2f}")
col2.metric(" متوسط المدة", f"{df['trip_duration_min'].mean():.1f} دقيقة")
col3.metric(" متوسط المسافة", f"{df['trip_distance'].mean():.1f} ميل")
col4.metric(" متوسط مكافأة خدمة", f"${df['tip_amount'].mean():.2f}")

st.divider()

# Charts Row 1
col1, col2 = st.columns(2)

with col1:
    st.subheader(" عدد الرحلات حسب الساعة")
    hourly = df.groupby("pickup_hour").size().reset_index(name="عدد الرحلات")
    st.bar_chart(hourly.set_index("pickup_hour"))

with col2:
    st.subheader(" متوسط السعر حسب الساعة")
    price_hourly = df.groupby("pickup_hour")["total_amount"].mean().reset_index()
    price_hourly.columns = ["pickup_hour", "متوسط السعر"]
    st.line_chart(price_hourly.set_index("pickup_hour"))

st.divider()

# Charts Row 2
col1, col2 = st.columns(2)

with col1:
    st.subheader(" توزيع مسافة الرحلات")
    dist = df[df["trip_distance"] < 20]["trip_distance"].value_counts().sort_index()
    st.bar_chart(dist)

with col2:
    st.subheader(" الرحلات حسب عدد الركاب")
    passengers = df["passenger_count"].value_counts().sort_index()
    st.bar_chart(passengers)

st.divider()

# Raw Data
st.subheader("📋 عينة من البيانات")
st.dataframe(df[[
    "tpep_pickup_datetime",
    "trip_distance",
    "trip_duration_min",
    "total_amount",
    "tip_amount",
    "pickup_hour"
]].head(100))