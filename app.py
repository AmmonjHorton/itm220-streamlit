import streamlit as st
import pandas as pd
import mysql.connector
import os
import hashlib
from mysql.connector import Error
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import altair as alt
load_dotenv()

mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_password_local = os.getenv("MYSQL_PASSWORD_LOCAL")

# requirement queries:

queries = {
    "Scriptures by Year": """
        SELECT yi.year_of_event, age, book, chapter, verse,
        CASE
        WHEN yi.age = 'BC' THEN -yi.year_of_event
        ELSE yi.year_of_event
        END AS year_timeline
        FROM year_info AS yi
        INNER JOIN scripture_study AS ss
        ON yi.scripture_study_id = ss.id
        ORDER BY year_timeline
    """,
    "Doctrine Counts": """
        SELECT doctrine_name, COUNT(*) AS count
        FROM timeline
        GROUP BY doctrine_name
    """,
    "Conditional Logic Example": """
    SELECT volume, book, chapter, verse,
    CASE
        WHEN volume = 'Old Testament' THEN 'OT'
        WHEN volume = 'New Testament' THEN 'NT'
        WHEN volume = 'Book of Mormon' THEN 'BOM'
        WHEN volume = 'Doctrine and Covenants' THEN 'D&C'
        WHEN volume = 'Pearl of Great Price' THEN 'PGP'
        ELSE 'Other'
        END AS volume_abbr
    FROM scripture_study
    ORDER BY volume_abbr
    """,
    "Outer Join Example": """
    SELECT ss.volume, ss.book, ss.chapter, ss.verse, yi.year_of_event, d.doctrine_name
    FROM scripture_study AS ss
    LEFT OUTER JOIN year_info AS yi ON ss.id = yi.scripture_study_id
    LEFT OUTER JOIN doctrine AS d ON ss.id = d.scripture_study_id
    ORDER BY ss.volume, ss.book
    """,
    "Group By Example": """
    SELECT ss.volume, COUNT(*) AS scripture_count
    FROM scripture_study AS ss
    GROUP BY ss.volume
    ORDER BY scripture_count DESC 
""",
    "Subqueries Example": """
    SELECT volume, book, chapter, verse
    FROM scripture_study
    WHERE id IN (
        SELECT scripture_study_id
        FROM year_info
        WHERE age = 'BC'
    )
    ORDER BY volume, book    
""",    ### I want to want this query to return the percentage of entries
        ###each volume takes up out of the total scripture_study entries.
    "Window Functions Example": """
    SELECT
        volume,
        volume_count,
        volume_count / SUM(volume_count) OVER () * 100 AS pct_of_total
    FROM (SELECT
        volume,
        COUNT(*) AS volume_count
    FROM scripture_study
    GROUP BY volume) AS volume_counts
    ORDER BY pct_of_total DESC;

    """

}


# Set up SSH tunnel


# ---------- Database Connection ----------
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
            password=mysql_password_local
        )
        return conn, server
    except Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None
    
@st.cache_data
def load_timeline():
    conn, tunnel = get_connection()
    df = pd.read_sql("""
SELECT timeline_year, year_of_event, age,
volume, book, chapter, verse, doctrine_name
FROM timeline
ORDER BY timeline_year
""", conn)
    conn.close()
    tunnel.stop()
    return df


@st.cache_data
def load_scriptures():
    """Load scripture_study rows (keeps function name for UI compatibility).
    Returns columns: volume, book, chapter, verse
    """
    conn, tunnel = get_connection()
    df = pd.read_sql("""
        SELECT id, volume, book, chapter, verse
        FROM scripture_study
        ORDER BY id
        """, conn)
    conn.close()
    tunnel.stop()
    return df

# Helper function to deduplicate columns
def dedupe_columns(df):
    seen = {}
    new_columns = []

    for col in df.columns:
        if col not in seen:
            seen[col] = 0
            new_columns.append(col)
        else:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")

    df.columns = new_columns
    return df


# =============================
# Run Query Function
# =============================
@st.cache_data(show_spinner=False)
def run_query(sql: str, limit: int):
    """
    Executes a SQL query with a LIMIT clause
    and returns a pandas DataFrame.
    """
    if limit:
        sql = f"{sql} LIMIT {limit}"

    conn, tunnel = get_connection()
    df = pd.read_sql(sql, conn)
    conn.close()
    tunnel.stop()
    # Auto-fix duplicate columns (student-friendly)
    df = dedupe_columns(df)

    return df



@st.cache_data
def load_chart_data():
    conn, tunnel = get_connection()
    df = pd.read_sql("""
        SELECT timeline_year, year_of_event, age, book
        FROM timeline
        ORDER BY timeline_year
        """, conn)
    conn.close()
    tunnel.stop()
    return df

def update_rows(updated_df, original_df):
    conn, tunnel = get_connection()
    cursor = conn.cursor()
    
    for i, row in updated_df.iterrows():
        original_row = original_df.loc[i]
        if not row.equals(original_row):
            cursor.execute(
                "UPDATE scripture_study SET volume=%s, book=%s, chapter=%s, verse=%s WHERE id=%s",
                (row.get('volume'), row.get('book'), row.get('chapter'), row.get('verse'), row.get('id'))
            )
    conn.commit()
    conn.close()
    tunnel.stop()

def delete_rows(ids_to_delete):
    if not ids_to_delete:
        return
    placeholders = ",".join(["%s"] * len(ids_to_delete))
    conn, tunnel = get_connection()
    cursor = conn.cursor()
    sql = f"DELETE FROM scripture_study WHERE id IN ({placeholders})"
    cursor.execute(sql, tuple(ids_to_delete))
    conn.commit()
    conn.close()
    tunnel.stop()
    try:
        st.cache_data.clear()
    except Exception:
        pass

def insert_row(volume, book, chapter, verse, age=None, year_of_event=None, doctrine_name=None):
    # Insert a scripture_study row and optionally related year_info and doctrine rows
    conn, tunnel = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO scripture_study (volume, book, chapter, verse) VALUES (%s, %s, %s, %s)",
        (volume, book, chapter, verse),
    )
    scripture_id = cursor.lastrowid

    # Insert year_info if both year and age are provided
    try:
        if year_of_event and age:
            # Ensure year_of_event is numeric if supplied as string
            cursor.execute(
                "INSERT INTO year_info (year_of_event, age, scripture_study_id) VALUES (%s, %s, %s)",
                (int(year_of_event), age, scripture_id),
            )
    except Exception:
        # If conversion fails or insert fails, ignore and continue
        pass

    # Insert doctrine if provided
    if doctrine_name:
        try:
            cursor.execute(
                "INSERT INTO doctrine (scripture_study_id, doctrine_name) VALUES (%s, %s)",
                (scripture_id, doctrine_name),
            )
        except Exception:
            pass

    conn.commit()
    conn.close()
    tunnel.stop()
    # Clear cached loads so the new row appears on next read
    try:
        st.cache_data.clear()
    except Exception:
        pass

# ---------- HELPER FUNCTIONS ----------

def hash_df(df):
    # Create a stable byte representation of the dataframe hashes
    h = pd.util.hash_pandas_object(df, index=True)
    return hashlib.md5(h.to_numpy().tobytes()).hexdigest()

# ---------- STREAMLIT APP ----------

st.title("Scripture Study Timeline Analysis")

# Line chart from DB view
st.subheader("Book Events Over Years")
# Build a layered Altair chart: line of event counts per year + markers for each event with labels
events = load_timeline()
if not events.empty:
    counts = events.groupby("timeline_year").size().reset_index(name="count")
    events = events.merge(counts, on="timeline_year")
    # Compose a readable label for each event
    events["label"] = events.apply(
        lambda r: f"{r.get('book','')} {r.get('chapter','')}:{r.get('verse','')} ({abs(r.get('year_of_event',0))} {r.get('age','')})",
        axis=1,
    )

    line = alt.Chart(counts).mark_line(point=True).encode(
        x=alt.X("timeline_year:Q", title="Year"),
        y=alt.Y("count:Q", title="Events"),
        tooltip=[alt.Tooltip("timeline_year:Q", title="Timeline Year"), alt.Tooltip("count:Q", title="Events")],
    )

    points = alt.Chart(events).mark_circle(color="#d62728", size=60).encode(
        x="timeline_year:Q",
        y="count:Q",
        tooltip=[
            alt.Tooltip("book:N", title="Book"),
            alt.Tooltip("chapter:N", title="Chapter"),
            alt.Tooltip("verse:N", title="Verse"),
            alt.Tooltip("year_of_event:Q", title="Year of Event"),
            alt.Tooltip("doctrine_name:N", title="Doctrine"),
        ],
    )

    # Text labels placed slightly above the point
    text = alt.Chart(events).mark_text(align="left", dx=5, dy=-10, fontSize=10).encode(
        x="timeline_year:Q",
        y="count:Q",
        text="label:N",
    )

    chart = alt.layer(line, points, text).interactive().properties(height=350)
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No timeline events to display.")


# Editable table
st.subheader("📜 Manage Scripture Entries (Add, Edit, Delete)")

# Load scripture_study table as the editable dataset
if "original_df" not in st.session_state:
    st.session_state.original_df = load_scriptures()
    st.session_state.original_hash = hash_df(st.session_state.original_df)

df = st.session_state.original_df.copy()
df["delete"] = False
edited_df = st.data_editor(
    df,
    num_rows="fixed",
    hide_index=True,
    column_order=("volume", "book", "chapter", "verse", "delete"),
    use_container_width=True,
)

# Delete selected rows
if st.button("🗑️ Delete Selected Rows"):
    selected_ids = edited_df[edited_df["delete"] == True]["id"].tolist()
    if selected_ids:
        print(f"Deleting rows with IDs: {selected_ids}")
        delete_rows(selected_ids)
        st.session_state.original_df = load_scriptures()
        st.session_state.original_hash = hash_df(st.session_state.original_df)
        st.success(f"Deleted {len(selected_ids)} row(s).")
        st.rerun()
    else:
        st.info("No rows selected for deletion.")

# Save edits
if st.button("💾 Save Edits"):
    edited_df = edited_df.drop(columns=["delete"])
    new_hash = hash_df(edited_df)
    if new_hash != st.session_state.original_hash:
        update_rows(edited_df, st.session_state.original_df)
        st.session_state.original_df = edited_df
        st.session_state.original_hash = new_hash
        st.success("Changes saved.")
    else:
        st.info("No changes detected.")

# Insert new row
st.subheader("➕ Add New Entry")
with st.form("insert_form"):
    new_volume = st.text_input("Volume")
    new_book = st.text_input("Book")
    new_chapter = st.text_input("Chapter")
    new_verse = st.text_input("Verse")
    new_doctrine = st.text_input("Doctrine Name")
    new_year = st.text_input("Year of Event")
    new_age = st.text_input("Age")
    submitted = st.form_submit_button("Add Entry")

    if submitted:
        if new_volume.strip() == "" or new_book.strip() == "":
            st.warning("Volume and Book are required (chapter/verse optional).")
        else:
            insert_row(
                new_volume.strip(),
                new_book.strip(),
                new_chapter.strip(),
                new_verse.strip(),
                new_age.strip(),
                new_year.strip(),
                new_doctrine.strip(),
            )
            st.session_state.original_df = load_scriptures()
            st.session_state.original_hash = hash_df(st.session_state.original_df)
            st.success(f"Scripture entry '{new_volume}' '{new_book}' added.")
            st.rerun()

# Selectbox for requirement queries with limit slider

# =============================
# UI for queries
# =============================

st.title("SQL Query Explorer")
selected_option = st.selectbox(
    "Choose a SQL concept:",
    options=list(queries.keys())
)

# Row limit control
row_limit = st.slider(
    "Row limit",
    min_value=10,
    max_value=1000,
    value=100,
    step=10
)
# Show SQL toggle
with st.expander("Preview SQL"):
    st.code(queries[selected_option], language="sql")

# Run Query Button
if st.button("Run Query", type="primary"):
    with st.spinner("Running query..."):
        try:
            df = run_query(queries[selected_option], row_limit)

            st.success(f"Results for **{selected_option}**")
            st.dataframe(df, use_container_width=True)

            st.caption(f"Rows returned: {len(df)}")

        except Exception as e:
            st.error("Query execution failed")
            st.exception(e)


# End of Streamlit app

