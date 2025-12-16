import streamlit as st
import pandas as pd
import mysql.connector
import os
from mysql.connector import Error
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

# =============================
# Environment & Secrets
# =============================
load_dotenv()
mysql_password_local = os.getenv("MYSQL_PASSWORD_LOCAL")

# =============================
# Database Connection
# =============================

def get_connection():
    try:
        server = SSHTunnelForwarder(
            (st.secrets["ssh"]["ssh_host"], 22),
            ssh_username=st.secrets["ssh"]["ssh_user"],
            ssh_pkey=st.secrets["ssh"]["ssh_pem_path"],
            remote_bind_address=(st.secrets["mysql"]["host"], st.secrets["mysql"]["port"]),
        )
        server.start()
        conn = mysql.connector.connect(
            host=st.secrets["mysql"]["host"],
            port=server.local_bind_port,
            database=st.secrets["mysql"]["database"],
            user=st.secrets["mysql"]["user"],
            password=mysql_password_local,
        )
        return conn, server
    except Error as e:
        st.error(f"Database connection failed: {e}")
        return None, None

# =============================
# Data Loaders
# =============================

@st.cache_data
def load_timeline():
    conn, tunnel = get_connection()
    df = pd.read_sql(
        """
        SELECT timeline_year, year_of_event, age,
               volume, book, chapter, verse, doctrine_name
        FROM timeline
        ORDER BY timeline_year
        """,
        conn,
    )
    conn.close()
    tunnel.stop()
    return df

# =============================
# App Layout
# =============================

st.set_page_config(page_title="Scripture Study Dashboard", layout="wide")

st.title("📖 Scripture Study Timeline Dashboard")
st.caption("Chronological scripture analysis using BC / AD normalization")

# =============================
# Filters
# =============================

data = load_timeline()

age_filter = st.multiselect(
    "Filter by Age",
    options=["BC", "AD"],
    default=["BC", "AD"],
)

filtered = data[data["age"].isin(age_filter)]

# =============================
# Timeline Chart
# =============================

st.subheader("📈 Scripture Mentions Over Time")

filtered["century"] = (filtered["timeline_year"] // 100) * 100
century_counts = filtered.groupby("century").size()

st.line_chart(century_counts)

# =============================
# Doctrine Breakdown
# =============================

st.subheader("📚 Doctrine Distribution")

doctrine_counts = filtered["doctrine_name"].value_counts()
st.bar_chart(doctrine_counts)

# =============================
# Scripture Table
# =============================

st.subheader("🗂️ Scripture Records")

filtered["display_year"] = filtered.apply(
    lambda r: f"{abs(r.year_of_event)} {r.age}", axis=1
)

st.dataframe(
    filtered[[
        "display_year",
        "volume",
        "book",
        "chapter",
        "verse",
        "doctrine_name",
    ]],
    use_container_width=True,
)

# =============================
# Summary Metrics
# =============================

st.subheader("📊 Summary")

col1, col2, col3 = st.columns(3)

col1.metric("Total Scriptures", len(filtered))
col2.metric("Books", filtered["book"].nunique())
col3.metric("Doctrines", filtered["doctrine_name"].nunique())
