"""
Data Dictionary App — User Guide
"""

import streamlit as st
from streamlit_local_storage import LocalStorage
from auth import check_password, restore_auth

_ls = LocalStorage()
restore_auth(_ls)
check_password()

st.set_page_config(page_title="Guide", page_icon="📚", layout="wide")

st.title("📚 Data Dictionary — User Guide")
st.caption("How to use the Data Dictionary app to document your Lakehouse")

# ────────────────────────────────────
# Overview
# ────────────────────────────────────
st.header("Overview")
st.markdown("""
The Data Dictionary app connects to your **Microsoft Fabric Lakehouse** and helps you:
- **Catalog** all tables and columns automatically
- **Document** business descriptions, owners, tags
- **Track** data quality metrics (null %, distinct values)
- **Generate code** to sync your documentation back to Fabric
""")

st.divider()

# ────────────────────────────────────
# Workflow
# ────────────────────────────────────
st.header("Workflow")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    ### 1️⃣ Scan
    Click **Scan Database** to read all tables and columns from the Lakehouse.
    """)

with col2:
    st.markdown("""
    ### 2️⃣ Document
    Add descriptions, business owners, tags to tables and columns.
    Use **AI Suggest** for auto-generated descriptions.
    """)

with col3:
    st.markdown("""
    ### 3️⃣ Generate
    Your edits are queued. Download the generated **SQL code** from the sidebar.
    """)

with col4:
    st.markdown("""
    ### 4️⃣ Apply
    Paste the code into a **Fabric notebook** and run it to save changes to `dd_tables` / `dd_columns`.
    """)

st.divider()

# ────────────────────────────────────
# Features
# ────────────────────────────────────
st.header("Features")

with st.expander("🔄 **Refresh Data**", expanded=False):
    st.markdown("""
    Reloads data from `dd_tables` and `dd_columns` tables.
    Clears all cached data so you see the latest version.

    **When to use:** After running generated code in Fabric notebook, click Refresh to see the updated data.
    """)

with st.expander("🔄 **Scan Database**", expanded=True):
    st.markdown("""
    Reads the structure of your Lakehouse and catalogs all tables and columns.

    **What it does:**
    | Step | Description | Queries |
    |------|------------|---------|
    | 1 | Get list of all tables from `INFORMATION_SCHEMA` | 1 query |
    | 2 | Get all column metadata (name, type, nullable) and primary keys | 2 queries |
    | 3 | Count rows (`COUNT(*)`) for each table | 1 per table |
    | **Total** | **For 80 tables** | **~83 queries** |

    **What it preserves:** descriptions, business owners, tags, and all other documentation you've added.
    These are merged back — scan never overwrites your edits.

    **Column stats** (null %, distinct count, sample values) are **not** loaded during scan.
    They load on-demand when you click on a specific table, keeping the scan fast.

    **For Fabric:** Generates MERGE SQL code that you download and run in a Fabric notebook.
    The code uses `MERGE INTO` so new tables are inserted, existing ones are updated.
    """)

with st.expander("🧹 **Cleanup Stale Records**", expanded=False):
    st.markdown("""
    Compares the tables currently in your Lakehouse against what's recorded in `dd_tables` / `dd_columns`.

    If a table or column was **deleted** from the Lakehouse but still exists in the dictionary,
    it shows up as "stale" and can be removed.

    **For Fabric:** Generates `DELETE` SQL code to run in a Fabric notebook.

    **When to use:** After dropping or renaming tables in the Lakehouse.
    """)

with st.expander("🤖 **AI Suggest**", expanded=False):
    st.markdown("""
    Uses **Groq AI** (free) to automatically generate:
    - **Table info:** description, business owner, source system, tags
    - **Column descriptions** and business-friendly names

    AI looks at column names, data types, sample values, and naming conventions
    (e.g., `dt_` = date, `amt_` = monetary, `is_` = boolean) to generate suggestions.

    Suggestions are pre-filled in the editor. Review and edit before saving.

    **Setup:** Enter your free Groq API key in the sidebar under AI Settings.
    Get one at [console.groq.com/keys](https://console.groq.com/keys).
    """)

with st.expander("📋 **Generate SQL Code**", expanded=False):
    st.markdown("""
    When you edit table or column descriptions on Fabric, changes are **queued locally**
    (Fabric Lakehouse SQL endpoint is read-only).

    The sidebar shows pending edits count and a **Download Code** button.
    The generated code contains `MERGE INTO` statements that you paste into a Fabric notebook.

    After running the code, click **Refresh Data** to see the updated values,
    then **Clear pending edits** to reset the queue.
    """)

st.divider()

# ────────────────────────────────────
# Architecture
# ────────────────────────────────────
st.header("Architecture")

st.markdown("""
```
┌─────────────────────┐     pyodbc (read-only)     ┌──────────────────────┐
│   Streamlit Cloud   │ ◄──────────────────────────► │   Fabric Lakehouse   │
│   (this app)        │      port 1433 / TDS        │   SQL Endpoint       │
└─────────────────────┘                              └──────────────────────┘
         │                                                      ▲
         │  Generate SQL code                                   │
         ▼                                                      │
┌─────────────────────┐     Spark SQL (read/write)  ┌──────────────────────┐
│   Download .py file │ ──────────────────────────► │   Fabric Notebook    │
│   (MERGE INTO...)   │                             │   (run generated     │
└─────────────────────┘                             │    code here)        │
                                                    └──────────────────────┘
```

**Why this architecture?**
- Fabric Lakehouse SQL endpoint is **read-only** — you can SELECT but not INSERT/UPDATE
- The app reads data via `pyodbc` over TDS (port 1433)
- Write operations go through **Spark** in Fabric notebooks
- Generated code uses `MERGE INTO` for safe upserts
""")

st.divider()

# ────────────────────────────────────
# Data Model
# ────────────────────────────────────
st.header("Data Model")

col1, col2 = st.columns(2)

with col1:
    st.subheader("dd_tables")
    st.markdown("""
    | Column | Description |
    |--------|------------|
    | `table_name` | Primary key |
    | `layer` | brz / slv / gld / ref / dq / utl |
    | `row_count` | Number of rows |
    | `column_count` | Number of columns |
    | `description` | Business description |
    | `business_owner` | Owner / team |
    | `source_system` | Source system |
    | `refresh_frequency` | How often data refreshes |
    | `tags` | Comma-separated tags |
    | `scanned_at` | Last scan timestamp |
    | `updated_at` | Last edit timestamp |
    | `updated_by` | Who edited |
    """)

with col2:
    st.subheader("dd_columns")
    st.markdown("""
    | Column | Description |
    |--------|------------|
    | `table_name` | FK to dd_tables |
    | `column_name` | Part of composite PK |
    | `data_type` | SQL data type |
    | `ordinal_position` | Column order |
    | `is_nullable` | YES / NO |
    | `is_primary_key` | Boolean |
    | `description` | Business description |
    | `business_name` | Human-readable name |
    | `sample_values` | Example values |
    | `scanned_at` | Last scan timestamp |
    | `updated_at` | Last edit timestamp |
    | `updated_by` | Who edited |
    """)

st.divider()

# ────────────────────────────────────
# Layer Convention
# ────────────────────────────────────
st.header("Layer Convention")

layers = {
    "brz": ("Bronze", "#78350f", "#fbbf24", "Raw data ingested from source systems"),
    "slv": ("Silver", "#1e3a5f", "#7dd3fc", "Cleaned, validated, and conformed data"),
    "gld": ("Gold", "#713f12", "#fde047", "Business-level aggregates and metrics"),
    "ref": ("Reference", "#14532d", "#86efac", "Lookup tables, master data, mappings"),
    "dq": ("Data Quality", "#4a1d6e", "#c084fc", "Data quality rules and results"),
    "utl": ("Utility", "#334155", "#94a3b8", "Helper tables, configs, logs"),
}

cols = st.columns(len(layers))
for i, (prefix, (name, bg, text_color, desc)) in enumerate(layers.items()):
    with cols[i]:
        st.markdown(
            f'<div style="background:{bg};padding:12px;border-radius:8px;text-align:center">'
            f'<div style="font-weight:700;font-size:16px;color:{text_color}">{prefix.upper()}</div>'
            f'<div style="font-size:12px;color:{text_color}">{name}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.caption(desc)
