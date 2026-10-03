import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from snowflake.connector import connect

st.set_page_config(page_title="SaaS Analytics", layout="wide")
st.title("📊 SaaS Product Analytics")

# Connection
@st.cache_resource
def get_connection():
    return connect(
        account="db89749.eu-west-2.aws",
        user="BARNAP",
        password="Halo89Qalo29Ko",
        role="TRANSFORMER",
        warehouse="TRANSFORMING",
        database="ANALYTICS",
        schema="BRONZE",
    )

conn = get_connection()
cur = conn.cursor()

# Sidebar filters
st.sidebar.header("Filters")
plan_tier = st.sidebar.multiselect("Plan Tier", ["free", "pro", "enterprise"], default=["free", "pro", "enterprise"])
device_type = st.sidebar.multiselect("Device", ["desktop", "mobile", "tablet"], default=["desktop", "mobile", "tablet"])
country = st.sidebar.multiselect("Country", ["US", "IN", "GB", "DE", "CA", "AU"], default=["US", "IN", "GB", "DE", "CA", "AU"])

# Load fact_sessions
cur.execute("""
    SELECT * FROM fact_sessions
    WHERE plan_tier IN ({})
      AND device_type IN ({})
      AND country IN ({})
""".format(
    ",".join(f"'{p}'" for p in plan_tier),
    ",".join(f"'{d}'" for d in device_type),
    ",".join(f"'{c}'" for c in country),
))
cols = [desc[0] for desc in cur.description]
df = pd.DataFrame(cur.fetchall(), columns=cols)
df.columns = df.columns.str.lower()   # ← add this

# Load dim_users
cur.execute("SELECT * FROM dim_users")
cols_u = [desc[0] for desc in cur.description]
users = pd.DataFrame(cur.fetchall(), columns=cols_u)
users.columns = users.columns.str.lower()   # ← add this

# KPIs
st.subheader("Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Sessions", f"{df['session_id'].nunique():,}")
col2.metric("Total Events", f"{len(df):,}")
col3.metric("Unique Users", f"{df['user_id'].nunique():,}")
col4.metric("Avg Events/Session", f"{len(df) / df['session_id'].nunique():.1f}")

# Event type breakdown
st.subheader("Events by Type")
event_counts = df["event_type"].value_counts()
st.bar_chart(event_counts)

# Feature adoption
st.subheader("Feature Adoption")
feature_counts = df["feature_used"].value_counts().dropna()
st.bar_chart(feature_counts)

# Device breakdown
st.subheader("Events by Device")
fig, ax = plt.subplots()
df["device_type"].value_counts().plot.pie(ax=ax, autopct="%1.1f%%")
ax.set_title("Events by Device")
st.pyplot(fig)

# Country breakdown
st.subheader("Events by Country")
st.bar_chart(df["country"].value_counts())

# Raw data
st.subheader("Raw Events (first 100)")
st.dataframe(df.head(100))   
