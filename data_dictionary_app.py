"""
Data Dictionary App - Streamlit UI
Connect to Local SQL Server or Fabric Lakehouse, scan all tables, display & edit metadata.

Run: streamlit run data_dictionary_app.py
"""

import os
import struct

import pyodbc
import streamlit as st
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from streamlit_local_storage import LocalStorage
from auth import check_password, restore_auth

ls = LocalStorage()
restore_auth(ls)
check_password()

# Auto-detect ODBC driver: prefer 18 (local), fallback to 17 (Streamlit Cloud)
_ODBC_DRIVER = None
for _drv in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
    if _drv in pyodbc.drivers():
        _ODBC_DRIVER = _drv
        break
if not _ODBC_DRIVER:
    st.error("No SQL Server ODBC driver found. Install ODBC Driver 17 or 18.")
    st.stop()

# ════════════════════════════════════════
# Page Config
# ════════════════════════════════════════
st.set_page_config(
    page_title="Data Dictionary",
    page_icon="📖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ════════════════════════════════════════
# Environment Configs (sensitive values from env vars)
# ════════════════════════════════════════
FABRIC_SERVER = os.environ.get("FABRIC_SERVER", "")
FABRIC_DATABASE = os.environ.get("FABRIC_DATABASE", "")
LOCAL_SERVER = os.environ.get("LOCAL_SERVER", ".")
LOCAL_DATABASE = os.environ.get("LOCAL_DATABASE", "")

ENV_CONFIGS = {
    "fabric_dev": {
        "label": "Fabric - Dev",
        "fabric": True,
        "database": FABRIC_DATABASE,
        "odbc": (
            f"DRIVER={{{_ODBC_DRIVER}}};"
            f"Server={FABRIC_SERVER},1433;"
            f"Database={FABRIC_DATABASE};"
            "Encrypt=yes;"
            "TrustServerCertificate=no"
        ),
    },
    "fabric_prod": {
        "label": "Fabric - Prod",
        "fabric": True,
        "database": FABRIC_DATABASE,
        "odbc": (
            f"DRIVER={{{_ODBC_DRIVER}}};"
            f"Server={FABRIC_SERVER},1433;"
            f"Database={FABRIC_DATABASE};"
            "Encrypt=yes;"
            "TrustServerCertificate=no"
        ),
    },
    "local": {
        "label": "Local SQL Server",
        "fabric": False,
        "odbc": (
            "DRIVER={SQL Server};"
            f"Server={LOCAL_SERVER};"
            f"Database={LOCAL_DATABASE};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
            "Command Timeout=0"
        ),
    },
}

# Azure AD token
SQL_COPT_SS_ACCESS_TOKEN = 1256  # pyodbc constant for access token


@st.cache_resource(ttl=2400)  # token valid ~1hr, refresh every 40min
def _get_fabric_token():
    """Get Azure AD token for Fabric SQL endpoint using azure-identity."""
    from azure.identity import DefaultAzureCredential
    credential = DefaultAzureCredential()
    token = credential.get_token("https://database.windows.net/.default")
    return token.token


# ════════════════════════════════════════
# Connection
# ════════════════════════════════════════
def _is_fabric() -> bool:
    return st.session_state.env.startswith("fabric")


def _get_fabric_connection():
    """Get or create a connection to Fabric with Azure AD token via pyodbc."""
    conn = st.session_state.get("_fabric_conn")
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    token = _get_fabric_token()
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    odbc_string = ENV_CONFIGS[st.session_state.env]["odbc"]
    conn = pyodbc.connect(odbc_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

    st.session_state["_fabric_conn"] = conn
    return conn


@st.cache_resource
def get_engine(env_key: str):
    """SQLAlchemy engine for local SQL Server only."""
    odbc_string = ENV_CONFIGS[env_key]["odbc"]
    return create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_string}")


def run_query(sql: str) -> pd.DataFrame:
    if _is_fabric():
        conn = _get_fabric_connection()
        return pd.read_sql(sql, conn)
    with get_engine(st.session_state.env).connect() as conn:
        return pd.read_sql(text(sql), conn)


def run_non_query(sql: str):
    if _is_fabric():
        conn = _get_fabric_connection()
        conn.execute(sql)
        conn.commit()
        return
    with get_engine(st.session_state.env).connect() as conn:
        conn.execute(text(sql))
        conn.commit()


def run_non_query_params(sql: str, params: dict):
    if _is_fabric():
        import re
        param_names = re.findall(r":(\w+)", sql)
        sql_qmark = re.sub(r":(\w+)", "?", sql)
        values = [params[name] for name in param_names]
        conn = _get_fabric_connection()
        conn.execute(sql_qmark, values)
        conn.commit()
        return
    with get_engine(st.session_state.env).connect() as conn:
        conn.execute(text(sql), params)
        conn.commit()


# ════════════════════════════════════════
# Constants & Layer Detection
# ════════════════════════════════════════
LAYER_PREFIXES = {
    "brz": "brz",
    "slv": "slv", "gld": "gld", "ref": "ref",
    "dq": "dq", "dd": "dd", "utl": "utl",
}
EXCLUDE_PREFIXES = ["dd_", "brz2"]

LAYER_COLORS = {
    "brz": "#fbbf24", "slv": "#7dd3fc", "gld": "#fde047",
    "ref": "#86efac", "dq": "#c084fc", "utl": "#94a3b8", "other": "#94a3b8",
}


def _safe_get(row, key, default=None):
    """Safely get a value from a pandas row, returning default if key missing or NaN."""
    try:
        val = row[key]
        return default if pd.isna(val) else val
    except (KeyError, IndexError):
        return default


def detect_layer(table_name: str) -> str:
    for prefix, layer in LAYER_PREFIXES.items():
        if table_name.startswith(prefix + "_"):
            return layer
    return "other"


# ════════════════════════════════════════
# DDL: Ensure dd_tables & dd_columns exist
# ════════════════════════════════════════
def ensure_dd_tables():
    if _is_fabric():
        # Fabric SQL endpoint is read-only for DDL via ODBC.
        # dd_tables/dd_columns must be created in Fabric notebook.
        # Try both with and without dbo schema.
        last_err = None
        for prefix in ("dbo.", f"{FABRIC_DATABASE}.dbo.", ""):
            try:
                run_query(f"SELECT TOP 1 table_name FROM {prefix}dd_tables")
                return  # found
            except Exception as e:
                last_err = e
                continue
        # Show actual error to help debug
        st.warning(
            f"Cannot read `dd_tables` from Fabric. Error: `{last_err}`\n\n"
            "Possible causes:\n"
            "- Tables `dd_tables`/`dd_columns` not created yet → run `nb_dd_run` notebook in Fabric\n"
            "- Connection issue → check ODBC Driver 18, Azure AD login\n"
            "- SQL endpoint not enabled for this Lakehouse"
        )
        return

    # Local SQL Server - create if not exists
    run_non_query("""
        IF OBJECT_ID('dbo.dd_tables', 'U') IS NULL
        CREATE TABLE dbo.dd_tables (
            table_name        NVARCHAR(255) NOT NULL PRIMARY KEY,
            layer             NVARCHAR(50)  NOT NULL,
            row_count         BIGINT,
            column_count      INT,
            description       NVARCHAR(MAX),
            business_owner    NVARCHAR(255),
            source_system     NVARCHAR(255),
            refresh_frequency NVARCHAR(100),
            tags              NVARCHAR(MAX),
            scanned_at        NVARCHAR(50)  NOT NULL,
            updated_at        NVARCHAR(50),
            updated_by        NVARCHAR(255)
        )
    """)
    run_non_query("""
        IF OBJECT_ID('dbo.dd_columns', 'U') IS NULL
        CREATE TABLE dbo.dd_columns (
            table_name       NVARCHAR(255) NOT NULL,
            column_name      NVARCHAR(255) NOT NULL,
            data_type        NVARCHAR(100) NOT NULL,
            ordinal_position INT           NOT NULL,
            is_nullable      NVARCHAR(10),
            is_primary_key   BIT DEFAULT 0,
            description      NVARCHAR(MAX),
            business_name    NVARCHAR(255),
            sample_values    NVARCHAR(MAX),
            null_percentage  FLOAT,
            distinct_count   BIGINT,
            scanned_at       NVARCHAR(50)  NOT NULL,
            updated_at       NVARCHAR(50),
            updated_by       NVARCHAR(255),
            PRIMARY KEY (table_name, column_name)
        )
    """)


# ════════════════════════════════════════
# Scanner
# ════════════════════════════════════════
def list_tables() -> list[str]:
    df = run_query("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """)
    names = df["TABLE_NAME"].tolist()
    return [n for n in names if not any(n.startswith(p) for p in EXCLUDE_PREFIXES)]


def _batch_load_metadata() -> tuple[pd.DataFrame, dict[str, set[str]]]:
    """Load ALL column metadata and primary keys in 2 queries (instead of per-table)."""
    all_cols = run_query("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """)

    pks: dict[str, set[str]] = {}
    try:
        pk_df = run_query("""
            SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = 'dbo'
        """)
        for _, row in pk_df.iterrows():
            pks.setdefault(row["TABLE_NAME"], set()).add(row["COLUMN_NAME"])
    except Exception:
        pass

    return all_cols, pks


def scan_one_table(table_name: str, all_cols_df: pd.DataFrame = None) -> tuple[dict, pd.DataFrame]:
    now = datetime.now().isoformat()

    if all_cols_df is not None:
        cols = all_cols_df[all_cols_df["TABLE_NAME"] == table_name].copy()
    else:
        cols = run_query(f"""
            SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)

    rc = run_query(f"SELECT COUNT(*) AS cnt FROM [dbo].[{table_name}]")
    row_count = int(rc["cnt"].iloc[0])

    tbl_meta = {
        "table_name": table_name,
        "layer": detect_layer(table_name),
        "row_count": row_count,
        "column_count": len(cols),
        "scanned_at": now,
    }
    return tbl_meta, cols


def scan_columns(table_name: str, cols_df: pd.DataFrame,
                 pks: set[str] = None) -> list[dict]:
    """Fast scan: metadata only. Stats + samples loaded on-demand via load_column_stats()."""
    now = datetime.now().isoformat()
    if pks is None:
        try:
            pk_df = run_query(f"""
                SELECT kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE tc.TABLE_NAME = '{table_name}'
                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """)
            pks = set(pk_df["COLUMN_NAME"].tolist()) if not pk_df.empty else set()
        except Exception:
            pks = set()
    results = []

    if cols_df.empty:
        return results

    cn_col = "COLUMN_NAME" if "COLUMN_NAME" in cols_df.columns else "column_name"
    dt_col = "DATA_TYPE" if "DATA_TYPE" in cols_df.columns else "data_type"
    op_col = "ORDINAL_POSITION" if "ORDINAL_POSITION" in cols_df.columns else "ordinal_position"
    in_col = "IS_NULLABLE" if "IS_NULLABLE" in cols_df.columns else "is_nullable"

    for _, row in cols_df.iterrows():
        cn = row[cn_col]
        results.append({
            "table_name": table_name,
            "column_name": cn,
            "data_type": row[dt_col],
            "ordinal_position": int(row[op_col]),
            "is_nullable": row[in_col],
            "is_primary_key": 1 if cn in pks else 0,
            "null_percentage": None,
            "distinct_count": None,
            "sample_values": None,
            "scanned_at": now,
        })

    return results


@st.cache_data(ttl=3600, show_spinner="Loading column stats...")
def load_column_stats(env: str, table_name: str) -> pd.DataFrame:
    """On-demand: load null%, distinct count, sample values for a single table.
    Merges into existing dd_columns data. Only 2 queries per table.
    """
    dd_cols = _query_dd_columns(env, table_name)
    if dd_cols.empty:
        return dd_cols

    col_names = dd_cols["column_name"].tolist()
    row_count_df = run_query(f"SELECT COUNT(*) AS cnt FROM [dbo].[{table_name}]")
    row_count = int(row_count_df["cnt"].iloc[0])

    # ── Stats: null counts + distinct counts ──
    stats_row = None
    if row_count > 0 and col_names:
        agg_parts = []
        for cn in col_names:
            agg_parts.append(
                f"SUM(CASE WHEN [{cn}] IS NULL THEN 1 ELSE 0 END) AS [{cn}__nulls], "
                f"COUNT(DISTINCT [{cn}]) AS [{cn}__dist]"
            )
        try:
            stats = run_query(f"SELECT {', '.join(agg_parts)} FROM [dbo].[{table_name}]")
            stats_row = stats.iloc[0]
        except Exception:
            pass

    # ── Samples ──
    samples = {}
    if col_names:
        try:
            sample_parts = []
            for cn in col_names:
                safe = cn.replace(' ', '_').replace('.', '_')
                sample_parts.append(
                    f"(SELECT STRING_AGG(val, ' | ') FROM "
                    f"(SELECT DISTINCT TOP 5 CAST([{cn}] AS NVARCHAR(200)) AS val "
                    f"FROM [dbo].[{table_name}] WHERE [{cn}] IS NOT NULL) AS s_{safe}) "
                    f"AS [{cn}__sample]"
                )
            sample_df = run_query(f"SELECT {', '.join(sample_parts)}")
            if not sample_df.empty:
                sample_row = sample_df.iloc[0]
                for cn in col_names:
                    val = sample_row.get(f"{cn}__sample")
                    if pd.notna(val) and str(val).strip():
                        samples[cn] = str(val)
        except Exception:
            pass

    # Merge stats into dd_cols
    df = dd_cols.copy()
    for idx, row in df.iterrows():
        cn = row["column_name"]
        if stats_row is not None and row_count > 0:
            try:
                nulls = int(stats_row[f"{cn}__nulls"])
                df.at[idx, "null_percentage"] = round(nulls / row_count * 100, 2)
                df.at[idx, "distinct_count"] = int(stats_row[f"{cn}__dist"])
            except Exception:
                pass
        if cn in samples:
            df.at[idx, "sample_values"] = samples[cn]

    return df


# ════════════════════════════════════════
# Save / Load
# ════════════════════════════════════════
def load_existing_table_descs() -> dict:
    try:
        df = run_query("""
            SELECT table_name, description, business_owner, source_system,
                   refresh_frequency, tags, updated_at, updated_by
            FROM dbo.dd_tables
        """)
        return {row["table_name"]: row.to_dict() for _, row in df.iterrows()}
    except Exception:
        return {}


def load_existing_col_descs() -> dict:
    try:
        df = run_query("""
            SELECT table_name, column_name, description, business_name,
                   is_primary_key, updated_at, updated_by
            FROM dbo.dd_columns
        """)
        return {
            (row["table_name"], row["column_name"]): row.to_dict()
            for _, row in df.iterrows()
        }
    except Exception:
        return {}


def save_scan_results(tables: list[dict], columns: list[dict]):
    old_tbl = load_existing_table_descs()
    old_col = load_existing_col_descs()

    # Merge table descriptions
    for t in tables:
        if t["table_name"] in old_tbl:
            old = old_tbl[t["table_name"]]
            for f in ("description", "business_owner", "source_system",
                       "refresh_frequency", "tags", "updated_at", "updated_by"):
                t[f] = old.get(f) or t.get(f)

    # Merge column descriptions
    for c in columns:
        key = (c["table_name"], c["column_name"])
        if key in old_col:
            old = old_col[key]
            for f in ("description", "business_name", "is_primary_key",
                       "updated_at", "updated_by"):
                c[f] = old.get(f) or c.get(f)

    # Upsert tables
    for t in tables:
        for k in ["table_name", "layer", "row_count", "column_count", "description",
                   "business_owner", "source_system", "refresh_frequency", "tags",
                   "scanned_at", "updated_at", "updated_by"]:
            t.setdefault(k, None)

        run_non_query_params("""
            MERGE dbo.dd_tables AS target
            USING (SELECT :table_name AS table_name) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET
                layer = :layer, row_count = :row_count, column_count = :column_count,
                description = :description, business_owner = :business_owner,
                source_system = :source_system, refresh_frequency = :refresh_frequency,
                tags = :tags, scanned_at = :scanned_at,
                updated_at = :updated_at, updated_by = :updated_by
            WHEN NOT MATCHED THEN INSERT
                (table_name, layer, row_count, column_count, description, business_owner,
                 source_system, refresh_frequency, tags, scanned_at, updated_at, updated_by)
            VALUES (:table_name, :layer, :row_count, :column_count, :description,
                    :business_owner, :source_system, :refresh_frequency, :tags,
                    :scanned_at, :updated_at, :updated_by);
        """, t)

    # Upsert columns
    for c in columns:
        for k in ["table_name", "column_name", "data_type", "ordinal_position",
                   "is_nullable", "is_primary_key", "description", "business_name",
                   "sample_values", "null_percentage", "distinct_count", "scanned_at",
                   "updated_at", "updated_by"]:
            c.setdefault(k, None)

        run_non_query_params("""
            MERGE dbo.dd_columns AS target
            USING (SELECT :table_name AS table_name, :column_name AS column_name) AS source
            ON target.table_name = source.table_name AND target.column_name = source.column_name
            WHEN MATCHED THEN UPDATE SET
                data_type = :data_type, ordinal_position = :ordinal_position,
                is_nullable = :is_nullable, is_primary_key = :is_primary_key,
                description = :description, business_name = :business_name,
                sample_values = :sample_values, null_percentage = :null_percentage,
                distinct_count = :distinct_count, scanned_at = :scanned_at,
                updated_at = :updated_at, updated_by = :updated_by
            WHEN NOT MATCHED THEN INSERT
                (table_name, column_name, data_type, ordinal_position, is_nullable,
                 is_primary_key, description, business_name, sample_values,
                 null_percentage, distinct_count, scanned_at, updated_at, updated_by)
            VALUES (:table_name, :column_name, :data_type, :ordinal_position,
                    :is_nullable, :is_primary_key, :description, :business_name,
                    :sample_values, :null_percentage, :distinct_count,
                    :scanned_at, :updated_at, :updated_by);
        """, c)


def _get_fabric_table_columns(table_name: str) -> set[str]:
    """Get actual column names of a table on Fabric (to avoid referencing missing columns)."""
    try:
        df = run_query(f"""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
        """)
        return set(df["COLUMN_NAME"].str.lower().tolist())
    except Exception:
        return set()


def save_scan_results_fabric(tables: list[dict], columns: list[dict]) -> str:
    """Generate Fabric notebook code for upserting scanned results."""
    old_tbl = load_existing_table_descs()
    old_col = load_existing_col_descs()

    # Merge existing descriptions into scanned data
    for t in tables:
        if t["table_name"] in old_tbl:
            old = old_tbl[t["table_name"]]
            for f in ("description", "business_owner", "source_system",
                       "refresh_frequency", "tags", "updated_at", "updated_by"):
                t[f] = old.get(f) or t.get(f)
    for c in columns:
        key = (c["table_name"], c["column_name"])
        if key in old_col:
            old = old_col[key]
            for f in ("description", "business_name", "is_primary_key",
                       "updated_at", "updated_by"):
                c[f] = old.get(f) or c.get(f)

    db = f"{FABRIC_DATABASE}.dbo"

    # Detect actual columns on Fabric to avoid referencing missing ones
    tbl_actual_cols = _get_fabric_table_columns("dd_tables")
    col_actual_cols = _get_fabric_table_columns("dd_columns")

    # All possible dd_tables keys
    all_tbl_keys = ["table_name", "layer", "row_count", "column_count", "description",
                    "business_owner", "source_system", "refresh_frequency", "tags",
                    "scanned_at", "updated_at", "updated_by"]
    if tbl_actual_cols:
        all_tbl_keys = [k for k in all_tbl_keys if k.lower() in tbl_actual_cols]

    # All possible dd_columns keys
    all_col_keys = ["table_name", "column_name", "data_type", "ordinal_position",
                    "is_nullable", "is_primary_key", "description", "business_name",
                    "sample_values", "null_percentage", "distinct_count", "scanned_at",
                    "updated_at", "updated_by"]
    if col_actual_cols:
        all_col_keys = [k for k in all_col_keys if k.lower() in col_actual_cols]

    # Prepare table data
    for t in tables:
        for k in all_tbl_keys:
            t.setdefault(k, None)
    # Prepare column data
    for c in columns:
        for k in all_col_keys:
            c.setdefault(k, None)

    # Build batch code using DataFrame + MERGE (2 operations instead of hundreds)
    tbl_update_keys = [k for k in all_tbl_keys if k != "table_name"]
    tbl_sets = ", ".join(f"target.{k} = source.{k}" for k in tbl_update_keys)
    tbl_cols = ", ".join(all_tbl_keys)
    tbl_src_cols = ", ".join(f"source.{k}" for k in all_tbl_keys)

    col_update_keys = [k for k in all_col_keys if k not in ("table_name", "column_name")]
    col_sets = ", ".join(f"target.{k} = source.{k}" for k in col_update_keys)
    col_cols = ", ".join(all_col_keys)
    col_src_cols = ", ".join(f"source.{k}" for k in all_col_keys)

    # Convert is_primary_key from int (0/1) to bool for Spark BooleanType
    for c in columns:
        if "is_primary_key" in c:
            c["is_primary_key"] = bool(c["is_primary_key"])

    # Serialize data as Python dicts
    import json as _json
    tbl_data_str = _json.dumps([{k: t[k] for k in all_tbl_keys} for t in tables], default=str)
    col_data_str = _json.dumps([{k: c[k] for k in all_col_keys} for c in columns], default=str)

    # Map column names to Spark types
    _spark_types = {
        "row_count": "LongType()", "column_count": "IntegerType()",
        "ordinal_position": "IntegerType()", "is_primary_key": "BooleanType()",
        "null_percentage": "DoubleType()", "distinct_count": "LongType()",
    }

    def _build_schema(keys):
        fields = []
        for k in keys:
            spark_type = _spark_types.get(k, "StringType()")
            fields.append(f"    StructField('{k}', {spark_type}, True)")
        return "StructType([\n" + ",\n".join(fields) + "\n])"

    tbl_schema_str = _build_schema(all_tbl_keys)
    col_schema_str = _build_schema(all_col_keys)

    lines = [
        "# ════════════════════════════════════════",
        "# Auto-generated SCAN results from Data Dictionary App",
        "# Paste into a Fabric notebook cell and run",
        "# ════════════════════════════════════════",
        "import json",
        "from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType, DoubleType",
        "",
        "# ── Tables ──",
        f"tbl_data = json.loads('''{tbl_data_str}''')",
        f"tbl_schema = {tbl_schema_str}",
        f"tbl_df = spark.createDataFrame(tbl_data, schema=tbl_schema)",
        f"tbl_df.createOrReplaceTempView('_dd_scan_tables')",
        f'spark.sql("""',
        f"    MERGE INTO {db}.dd_tables AS target",
        f"    USING _dd_scan_tables AS source",
        f"    ON target.table_name = source.table_name",
        f"    WHEN MATCHED THEN UPDATE SET {tbl_sets}",
        f"    WHEN NOT MATCHED THEN INSERT ({tbl_cols}) VALUES ({tbl_src_cols})",
        f'""")',
        f"print(f'Updated {{len(tbl_data)}} tables')",
        "",
        "# ── Columns ──",
        f"col_data = json.loads('''{col_data_str}''')",
        f"col_schema = {col_schema_str}",
        f"col_df = spark.createDataFrame(col_data, schema=col_schema)",
        f"col_df.createOrReplaceTempView('_dd_scan_columns')",
        f'spark.sql("""',
        f"    MERGE INTO {db}.dd_columns AS target",
        f"    USING _dd_scan_columns AS source",
        f"    ON target.table_name = source.table_name AND target.column_name = source.column_name",
        f"    WHEN MATCHED THEN UPDATE SET {col_sets}",
        f"    WHEN NOT MATCHED THEN INSERT ({col_cols}) VALUES ({col_src_cols})",
        f'""")',
        f"print(f'Updated {{len(col_data)}} columns')",
        "",
        f"print('Done: {len(tables)} tables, {len(columns)} columns')",
    ]
    return "\n".join(lines)


def cleanup_stale_records() -> tuple[list[str], list[tuple[str, str]]]:
    """Find tables/columns in dd_tables/dd_columns that no longer exist in the database.
    Returns (stale_tables, stale_columns).
    """
    current_tables = set(list_tables())

    dd_tables = load_dd_tables()
    stale_tables = []
    if not dd_tables.empty:
        for tn in dd_tables["table_name"].tolist():
            if tn not in current_tables:
                stale_tables.append(tn)

    dd_cols = load_dd_columns()
    stale_columns = []
    if not dd_cols.empty:
        for tn in dd_cols["table_name"].unique():
            if tn not in current_tables:
                for cn in dd_cols[dd_cols["table_name"] == tn]["column_name"].tolist():
                    stale_columns.append((tn, cn))

    return stale_tables, stale_columns


def generate_cleanup_code(stale_tables: list[str], stale_columns: list[tuple[str, str]]) -> str:
    """Generate Fabric notebook code to delete stale records using batch SQL."""
    db = f"{FABRIC_DATABASE}.dbo"
    lines = [
        "# ════════════════════════════════════════",
        "# Auto-generated CLEANUP from Data Dictionary App",
        "# Removes tables/columns no longer in the Lakehouse",
        "# ════════════════════════════════════════",
        "",
    ]

    # Batch delete stale tables
    if stale_tables:
        in_list = ", ".join(f"'{_sql_escape(tn)}'" for tn in stale_tables)
        lines.append(f"# Remove {len(stale_tables)} stale tables")
        lines.append(f"spark.sql(\"DELETE FROM {db}.dd_tables WHERE table_name IN ({in_list})\")")
        lines.append(f"spark.sql(\"DELETE FROM {db}.dd_columns WHERE table_name IN ({in_list})\")")
        lines.append(f"print('Removed {len(stale_tables)} stale tables')")
        lines.append("")

    # Batch delete stale columns (grouped by table)
    seen_tables = set(stale_tables)
    orphan_cols: dict[str, list[str]] = {}
    for tn, cn in stale_columns:
        if tn not in seen_tables:
            orphan_cols.setdefault(tn, []).append(cn)

    if orphan_cols:
        total_cols = sum(len(v) for v in orphan_cols.values())
        lines.append(f"# Remove {total_cols} stale columns from {len(orphan_cols)} tables")
        for tn, cols in orphan_cols.items():
            safe_tn = _sql_escape(tn)
            col_list = ", ".join(f"'{_sql_escape(cn)}'" for cn in cols)
            lines.append(
                f"spark.sql(\"DELETE FROM {db}.dd_columns "
                f"WHERE table_name = '{safe_tn}' AND column_name IN ({col_list})\")"
            )
        lines.append(f"print('Removed {total_cols} stale columns')")
        lines.append("")

    lines.append(f"print('Cleanup done: {len(stale_tables)} tables, {len(stale_columns)} columns removed')")
    return "\n".join(lines)


@st.cache_data(ttl=3600, show_spinner=False)
def _query_dd_tables(env: str) -> pd.DataFrame:
    """Cached query for dd_tables (5 min TTL)."""
    return run_query("SELECT * FROM dbo.dd_tables ORDER BY layer, table_name")


@st.cache_data(ttl=3600, show_spinner=False)
def _query_dd_columns(env: str, table_name: str | None = None) -> pd.DataFrame:
    """Cached query for dd_columns (5 min TTL)."""
    if table_name:
        return run_query(
            f"SELECT * FROM dbo.dd_columns WHERE table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        )
    return run_query("SELECT * FROM dbo.dd_columns ORDER BY table_name, ordinal_position")


def load_dd_tables() -> pd.DataFrame:
    try:
        df = _query_dd_tables(st.session_state.env)
        if _is_fabric():
            df = _apply_table_overrides(df)
        return df
    except Exception:
        return pd.DataFrame()


def load_dd_columns(table_name: str | None = None) -> pd.DataFrame:
    try:
        df = _query_dd_columns(st.session_state.env, table_name)
        if _is_fabric():
            df = _apply_column_overrides(df)
        return df
    except Exception:
        return pd.DataFrame()


def update_table_fields(table_name: str, fields: dict):
    if _is_fabric():
        _save_table_override(table_name, fields)
        return
    now = datetime.now().isoformat()
    fields["updated_at"] = now
    set_parts = ", ".join(f"{k} = :{k}" for k in fields)
    fields["_table_name"] = table_name
    run_non_query_params(
        f"UPDATE dbo.dd_tables SET {set_parts} WHERE table_name = :_table_name",
        fields,
    )


def update_column_fields(table_name: str, column_name: str, fields: dict):
    if _is_fabric():
        _save_column_override(table_name, column_name, fields)
        return
    now = datetime.now().isoformat()
    fields["updated_at"] = now
    set_parts = ", ".join(f"{k} = :{k}" for k in fields)
    fields["_table_name"] = table_name
    fields["_column_name"] = column_name
    run_non_query_params(
        f"UPDATE dbo.dd_columns SET {set_parts} "
        f"WHERE table_name = :_table_name AND column_name = :_column_name",
        fields,
    )


def export_json() -> str:
    tables = load_dd_tables()
    columns = load_dd_columns()
    result = []
    for _, row in tables.iterrows():
        t = row.to_dict()
        tbl_cols = columns[columns["table_name"] == row["table_name"]]
        t["columns"] = tbl_cols.to_dict("records")
        result.append(t)
    return json.dumps(result, indent=2, default=str)


# ════════════════════════════════════════
# Local Overrides (for Fabric read-only mode)
# ════════════════════════════════════════
OVERRIDES_FILE = Path(__file__).parent / "dd_overrides.json"


def _load_overrides() -> dict:
    """Load local overrides from JSON file (cached in session state)."""
    if "overrides" not in st.session_state:
        if OVERRIDES_FILE.exists():
            st.session_state.overrides = json.loads(OVERRIDES_FILE.read_text(encoding="utf-8"))
        else:
            st.session_state.overrides = {"tables": {}, "columns": {}}
    return st.session_state.overrides


def _save_overrides(data: dict):
    """Save local overrides to JSON file and update cache."""
    OVERRIDES_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    st.session_state.overrides = data


def _save_table_override(table_name: str, fields: dict):
    """Save table-level edits to local JSON."""
    ov = _load_overrides()
    ov["tables"].setdefault(table_name, {})
    ov["tables"][table_name].update({k: v for k, v in fields.items() if v})
    ov["tables"][table_name]["updated_at"] = datetime.now().isoformat()
    _save_overrides(ov)


def _save_column_override(table_name: str, column_name: str, fields: dict):
    """Save column-level edits to local JSON."""
    ov = _load_overrides()
    key = f"{table_name}::{column_name}"
    ov["columns"].setdefault(key, {"table_name": table_name, "column_name": column_name})
    ov["columns"][key].update({k: v for k, v in fields.items() if v is not None})
    ov["columns"][key]["updated_at"] = datetime.now().isoformat()
    _save_overrides(ov)


def _apply_table_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """Merge local overrides into dd_tables DataFrame."""
    ov = _load_overrides()
    if not ov["tables"] or df.empty:
        return df
    df = df.copy()
    for tbl_name, fields in ov["tables"].items():
        mask = df["table_name"] == tbl_name
        if mask.any():
            for k, v in fields.items():
                if k in df.columns:
                    df.loc[mask, k] = v
    return df


def _apply_column_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """Merge local overrides into dd_columns DataFrame."""
    ov = _load_overrides()
    if not ov["columns"] or df.empty:
        return df
    df = df.copy()
    for _, fields in ov["columns"].items():
        tbl = fields.get("table_name")
        col = fields.get("column_name")
        mask = (df["table_name"] == tbl) & (df["column_name"] == col)
        if mask.any():
            for k, v in fields.items():
                if k in df.columns and k not in ("table_name", "column_name"):
                    df.loc[mask, k] = v
    return df


def _sql_escape(val: str) -> str:
    """Escape single quotes for SQL string literals."""
    return str(val).replace("'", "''")


def _generate_fabric_code() -> str | None:
    """Generate self-contained Python code for Fabric notebook."""
    ov = _load_overrides()
    if not ov["tables"] and not ov["columns"]:
        return None

    db = f"{FABRIC_DATABASE}.dbo"
    now = datetime.now().isoformat()
    lines = [
        "# ════════════════════════════════════════",
        "# Auto-generated from Data Dictionary App",
        "# Paste into a Fabric notebook cell and run",
        "# ════════════════════════════════════════",
        "",
    ]

    # Table updates via Spark SQL MERGE
    _skip_keys = {"updated_at", "_table_name", "_column_name"}
    for tbl, fields in ov.get("tables", {}).items():
        clean = {k: _sql_escape(v) for k, v in fields.items() if k not in _skip_keys and v}
        if not clean:
            continue
        clean["updated_at"] = now
        set_parts = ", ".join(f"{k} = '{v}'" for k, v in clean.items())
        safe_tbl = _sql_escape(tbl)
        lines.append(f"spark.sql(\"\"\"")
        lines.append(f"    MERGE INTO {db}.dd_tables AS t")
        lines.append(f"    USING (SELECT '{safe_tbl}' AS table_name) AS s")
        lines.append(f"    ON t.table_name = s.table_name")
        lines.append(f"    WHEN MATCHED THEN UPDATE SET {set_parts}")
        lines.append(f"\"\"\")")
        lines.append(f"print('Updated table: {tbl}')")
        lines.append("")

    # Column updates via Spark SQL MERGE
    for _, fields in ov.get("columns", {}).items():
        tbl = fields["table_name"]
        col = fields["column_name"]
        clean = {k: _sql_escape(v) for k, v in fields.items()
                 if k not in ("table_name", "column_name", "updated_at", "_table_name", "_column_name") and v is not None}
        if not clean:
            continue
        clean["updated_at"] = now
        set_parts = ", ".join(f"{k} = '{v}'" for k, v in clean.items())
        safe_tbl = _sql_escape(tbl)
        safe_col = _sql_escape(col)
        lines.append(f"spark.sql(\"\"\"")
        lines.append(f"    MERGE INTO {db}.dd_columns AS t")
        lines.append(f"    USING (SELECT '{safe_tbl}' AS table_name, '{safe_col}' AS column_name) AS s")
        lines.append(f"    ON t.table_name = s.table_name AND t.column_name = s.column_name")
        lines.append(f"    WHEN MATCHED THEN UPDATE SET {set_parts}")
        lines.append(f"\"\"\")")
        lines.append(f"print('Updated column: {tbl}.{col}')")
        lines.append("")

    lines.append("print('Done!')")
    return "\n".join(lines)


def _clear_overrides():
    """Clear all local overrides after syncing to Fabric."""
    _save_overrides({"tables": {}, "columns": {}})


# ════════════════════════════════════════
# AI Suggestions (Groq - free)
# ════════════════════════════════════════
# Get free API key at: https://console.groq.com/keys
# Set env var: GROQ_API_KEY=your_key

GROQ_MODEL = "llama-3.3-70b-versatile"
_GROQ_LS_KEY = "dd_groq_api_key"


def _load_groq_key() -> str:
    """Load Groq API key from browser LocalStorage."""
    if "_groq_key" in st.session_state:
        return st.session_state["_groq_key"]
    val = ls.getItem(_GROQ_LS_KEY)
    if val:
        st.session_state["_groq_key"] = val.strip()
        return st.session_state["_groq_key"]
    # First render after reload: LocalStorage not ready yet, trigger rerun once
    if "_ls_loaded" not in st.session_state:
        st.session_state["_ls_loaded"] = True
        st.rerun()
    return ""


def _save_groq_key(key: str):
    """Save Groq API key to browser LocalStorage."""
    key = key.strip()
    ls.setItem(_GROQ_LS_KEY, key)
    st.session_state["_groq_key"] = key


def _call_ai(prompt: str, max_tokens: int = 2000) -> str:
    """Call Groq API."""
    import requests as req
    api_key = _load_groq_key()
    if not api_key:
        raise ValueError("No API key. Enter your Groq API key in the sidebar.")
    r = req.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.2,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()


def _parse_json_response(text: str):
    """Extract JSON from AI response (handles markdown code blocks)."""
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    return json.loads(text)


def ai_suggest_table(table_name: str, layer: str, columns_df: pd.DataFrame) -> dict:
    """Ask AI to suggest table metadata based on name and columns."""
    col_info = []
    for _, row in columns_df.head(30).iterrows():
        parts = [f"{row.get('column_name', '?')} ({row.get('data_type', '?')})"]
        sample = _safe_get(row, "sample_values")
        if sample:
            parts.append(f"samples: {sample}")
        col_info.append(" - ".join(parts))

    prompt = f"""You are a data dictionary assistant for a supply chain analytics lakehouse.

Table: {table_name}
Layer: {layer} (brz=raw/bronze, slv=cleaned/silver, gld=aggregated/gold, ref=reference/master)
Columns:
{chr(10).join(col_info)}

Based on the table name, layer, column names, data types and sample values, suggest:
1. description: A concise English business description (1-2 sentences)
2. business_owner: Which team likely owns this (e.g. "Supply Chain", "Sales", "Finance", "Analytics")
3. source_system: The likely source system
4. tags: Comma-separated relevant tags
5. refresh_frequency: One of: hourly, daily, weekly, monthly, ad-hoc

Respond in JSON format only, no explanation:
{{"description": "...", "business_owner": "...", "source_system": "...", "tags": "...", "refresh_frequency": "..."}}"""

    return _parse_json_response(_call_ai(prompt, 500))


def ai_suggest_columns(table_name: str, layer: str, columns_df: pd.DataFrame) -> list[dict]:
    """Ask AI to suggest column descriptions and business names."""
    col_info = []
    for _, row in columns_df.iterrows():
        parts = [row.get("column_name", "?"), row.get("data_type", "?")]
        sample = _safe_get(row, "sample_values")
        if sample:
            parts.append(f"samples: {sample}")
        null_pct = _safe_get(row, "null_percentage")
        if null_pct is not None:
            parts.append(f"null: {null_pct}%")
        col_info.append(" | ".join(parts))

    prompt = f"""You are a data dictionary assistant for a supply chain analytics lakehouse.

Table: {table_name} (layer: {layer})
Columns:
{chr(10).join(col_info)}

Column naming convention:
- id_* = identifiers/keys, code_* = category codes, name_* = descriptive text
- dt_* = date, ts_* = timestamp, amt_* = monetary, qty_* = quantity
- num_* = count, val_* = values, pct_* = percentage, is_* = boolean, sk_* = surrogate key

For each column, suggest:
- description: Concise English description
- business_name: Human-readable English business name

Respond as a JSON array only, no explanation:
[{{"column_name": "...", "description": "...", "business_name": "..."}}, ...]"""

    return _parse_json_response(_call_ai(prompt, 2000))


# ════════════════════════════════════════
# Custom CSS
# ════════════════════════════════════════
st.markdown("""
<style>
    .layer-badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 700;
        font-family: monospace;
    }
    .metric-card {
        background: #1e293b;
        border-radius: 8px;
        padding: 16px;
        text-align: center;
    }
    .metric-value {
        font-size: 28px;
        font-weight: 700;
        color: #e2e8f0;
    }
    .metric-label {
        font-size: 12px;
        color: #94a3b8;
        margin-top: 4px;
    }
    div[data-testid="stSidebar"] .stRadio label {
        font-size: 13px !important;
    }
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════
# Initialize session state
# ════════════════════════════════════════
if "env" not in st.session_state:
    st.session_state.env = "fabric_dev"
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None


# ════════════════════════════════════════
# Sidebar
# ════════════════════════════════════════
with st.sidebar:
    st.title("📖 Data Dictionary")

    # Environment selector
    env_options = list(ENV_CONFIGS.keys())
    env_labels = [ENV_CONFIGS[k]["label"] for k in env_options]
    current_idx = env_options.index(st.session_state.env)
    selected_env = st.selectbox(
        "Environment",
        env_options,
        index=current_idx,
        format_func=lambda k: ENV_CONFIGS[k]["label"],
    )
    if selected_env != st.session_state.env:
        st.session_state.env = selected_env
        st.session_state.selected_table = None
        st.rerun()

    st.caption(ENV_CONFIGS[st.session_state.env]["label"])

    # Ensure dd_tables exist for this env
    try:
        ensure_dd_tables()
    except Exception as e:
        st.error(f"Connection failed: `{type(e).__name__}: {e}`")
        st.stop()

    # Refresh button - clear all caches
    if st.button("🔄 Refresh Data", use_container_width=True,
                 help="Reload data from dd_tables/dd_columns (clear cache)"):
        _query_dd_tables.clear()
        _query_dd_columns.clear()
        load_column_stats.clear()
        # Clear row count caches
        for key in list(st.session_state.keys()):
            if key.startswith("row_count_"):
                del st.session_state[key]
        # Clear fabric connection
        if "_fabric_conn" in st.session_state:
            try:
                st.session_state["_fabric_conn"].close()
            except Exception:
                pass
            del st.session_state["_fabric_conn"]
        st.rerun()

    # Scan button — uses session state to track scanning progress
    if "scanning" not in st.session_state:
        st.session_state.scanning = False

    scan_col1, scan_col2 = st.columns([3, 1])
    with scan_col1:
        start_scan = st.button(
            "🔄 Scan Database", use_container_width=True, type="primary",
            disabled=st.session_state.scanning,
            help="Scan all tables and columns from the Lakehouse",
        )
    with scan_col2:
        stop_scan = st.button(
            "⏹", use_container_width=True,
            disabled=not st.session_state.scanning,
            help="Stop scanning",
        )

    if stop_scan:
        st.session_state.scanning = False
        st.rerun()

    if start_scan:
        st.session_state.scanning = True
        st.rerun()

    if st.session_state.scanning:
        tables_to_scan = list_tables()
        all_tables = []
        all_columns = []
        progress = st.progress(0, text="Loading metadata...")
        stopped = False

        # Batch load all column metadata + primary keys in 2 queries
        all_cols_df, all_pks = _batch_load_metadata()

        for i, name in enumerate(tables_to_scan):
            # Check if user requested stop
            if not st.session_state.scanning:
                stopped = True
                break

            progress.progress(
                (i + 1) / len(tables_to_scan),
                text=f"Scanning {name}... ({i+1}/{len(tables_to_scan)})",
            )
            try:
                tbl_meta, cols_df = scan_one_table(name, all_cols_df)
                all_tables.append(tbl_meta)
                col_results = scan_columns(name, cols_df,
                                           pks=all_pks.get(name, set()))
                all_columns.extend(col_results)
            except Exception as e:
                st.warning(f"Skip {name}: {e}")

        progress.empty()
        st.session_state.scanning = False

        if all_tables:
            if _is_fabric():
                code = save_scan_results_fabric(all_tables, all_columns)
                st.session_state["_scan_code"] = code
                msg = f"Scanned {len(all_tables)} tables, {len(all_columns)} columns."
                if stopped:
                    msg += " (stopped early)"
                msg += " Download code below."
                st.success(msg)
            else:
                save_scan_results(all_tables, all_columns)
                msg = f"Scanned {len(all_tables)} tables, {len(all_columns)} columns"
                if stopped:
                    msg += " (stopped early — partial results saved)"
                st.success(msg)
            _query_dd_tables.clear()
            _query_dd_columns.clear()
        elif stopped:
            st.info("Scan stopped before any tables were scanned.")
        st.rerun()

    # Show scan code download (Fabric)
    if _is_fabric() and "_scan_code" in st.session_state:
        st.download_button(
            "📥 Download Scan Code",
            data=st.session_state["_scan_code"],
            file_name="dd_scan_results.py",
            mime="text/x-python",
            use_container_width=True,
        )
        with st.expander("Preview scan code"):
            st.code(st.session_state["_scan_code"], language="python")
        if st.button("🗑 Clear scan code", use_container_width=True):
            del st.session_state["_scan_code"]
            st.rerun()

    # Cleanup stale records
    if st.button("🧹 Cleanup Stale Records", use_container_width=True,
                 help="Remove tables/columns no longer in the Lakehouse"):
        try:
            stale_tables, stale_columns = cleanup_stale_records()
            if not stale_tables and not stale_columns:
                st.info("No stale records found. Everything is up to date.")
            elif _is_fabric():
                code = generate_cleanup_code(stale_tables, stale_columns)
                st.session_state["_cleanup_code"] = code
                st.warning(f"Found {len(stale_tables)} stale tables, {len(stale_columns)} stale columns. Download code below.")
                st.rerun()
            else:
                # Local: delete directly
                for tn in stale_tables:
                    run_non_query(f"DELETE FROM dbo.dd_columns WHERE table_name = '{_sql_escape(tn)}'")
                    run_non_query(f"DELETE FROM dbo.dd_tables WHERE table_name = '{_sql_escape(tn)}'")
                for tn, cn in stale_columns:
                    if tn not in stale_tables:
                        run_non_query(f"DELETE FROM dbo.dd_columns WHERE table_name = '{_sql_escape(tn)}' AND column_name = '{_sql_escape(cn)}'")
                _query_dd_tables.clear()
                _query_dd_columns.clear()
                st.success(f"Removed {len(stale_tables)} tables, {len(stale_columns)} columns")
                st.rerun()
        except Exception as e:
            st.error(f"Cleanup failed: {e}")

    # Show cleanup code download (Fabric)
    if _is_fabric() and "_cleanup_code" in st.session_state:
        st.download_button(
            "📥 Download Cleanup Code",
            data=st.session_state["_cleanup_code"],
            file_name="dd_cleanup.py",
            mime="text/x-python",
            use_container_width=True,
        )
        with st.expander("Preview cleanup code"):
            st.code(st.session_state["_cleanup_code"], language="python")
        if st.button("🗑 Clear cleanup code", use_container_width=True):
            del st.session_state["_cleanup_code"]
            st.rerun()

    st.divider()

    # Load data
    dd_tables = load_dd_tables()

    if dd_tables.empty:
        st.info("No data yet. Click **Scan Database** to start.")
        st.stop()

    # Coverage stats
    total_tables = len(dd_tables)
    described_tables = dd_tables["description"].notna().sum() if "description" in dd_tables.columns else 0
    tbl_coverage = round(described_tables / total_tables * 100) if total_tables else 0

    col1, col2 = st.columns(2)
    col1.metric("Tables", total_tables)
    col2.metric("Coverage", f"{tbl_coverage}%")

    st.divider()

    with st.expander(f"📋 Tables ({total_tables})", expanded=True):
        # Search
        search = st.text_input("🔍 Search tables", placeholder="Type to filter...")

        # Layer filter
        layers = sorted(dd_tables["layer"].unique().tolist())
        layer_filter = st.multiselect("Filter by layer", layers, default=layers)

        # Filter tables
        filtered = dd_tables[dd_tables["layer"].isin(layer_filter)]
        if search:
            mask = (
                filtered["table_name"].str.contains(search, case=False, na=False)
                | filtered["description"].str.contains(search, case=False, na=False)
            )
            filtered = filtered[mask]

        # Table list
        table_names = filtered["table_name"].tolist()
        if table_names:
            # Build display labels with layer prefix
            labels = []
            for _, row in filtered.iterrows():
                layer_tag = row["layer"].upper()
                name = row["table_name"]
                rc = _safe_get(row, "row_count")
                rows = f"{int(rc):,}" if rc is not None else "?"
                labels.append(f"[{layer_tag}] {name} ({rows} rows)")

            selected_idx = st.radio(
                "Tables",
                range(len(table_names)),
                format_func=lambda i: labels[i],
                label_visibility="collapsed",
            )
            st.session_state.selected_table = table_names[selected_idx]
        else:
            st.warning("No tables match your filter.")
            st.session_state.selected_table = None

    st.divider()

    # Export
    st.download_button(
        "📥 Export JSON",
        data=export_json(),
        file_name="data_dictionary.json",
        mime="application/json",
        use_container_width=True,
    )

    # Fabric: show pending overrides & generate code
    if _is_fabric():
        ov = _load_overrides()
        n_tbl = len(ov["tables"])
        n_col = len(ov["columns"])
        if n_tbl or n_col:
            st.divider()
            st.warning(f"Pending edits: {n_tbl} tables, {n_col} columns")
            code = _generate_fabric_code()
            if code:
                st.download_button(
                    "📋 Download Code for Fabric",
                    data=code,
                    file_name="dd_updates.py",
                    mime="text/x-python",
                    use_container_width=True,
                )
                with st.expander("Preview generated code"):
                    st.code(code, language="python")
                if st.button("🗑 Clear pending edits", use_container_width=True):
                    _clear_overrides()
                    st.rerun()

    # Groq API key
    st.divider()
    saved_key = _load_groq_key()
    with st.expander("🤖 AI Settings", expanded=not saved_key):
        new_key = st.text_input(
            "Groq API Key",
            value=saved_key,
            type="password",
            placeholder="gsk_...",
            help="Free key: https://console.groq.com/keys",
        )
        if new_key != saved_key:
            _save_groq_key(new_key)
            st.success("API key saved!")
            st.rerun()



# ════════════════════════════════════════
# Main Area
# ════════════════════════════════════════
selected = st.session_state.selected_table

if selected is None:
    # Overview
    st.header("Database Overview")

    dd_all_cols = load_dd_columns()
    total_cols = len(dd_all_cols)
    described_cols = dd_all_cols["description"].notna().sum() if not dd_all_cols.empty else 0
    col_coverage = round(described_cols / total_cols * 100) if total_cols else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Tables", total_tables)
    m2.metric("Total Columns", total_cols)
    m3.metric("Table Coverage", f"{tbl_coverage}%")
    m4.metric("Column Coverage", f"{col_coverage}%")

    st.subheader("Tables by Layer")
    agg_dict = {"table_name": ("table_name", "count")}
    if "row_count" in dd_tables.columns:
        agg_dict["total_rows"] = ("row_count", "sum")
    if "column_count" in dd_tables.columns:
        agg_dict["avg_columns"] = ("column_count", "mean")
    layer_summary = dd_tables.groupby("layer").agg(**agg_dict).reset_index()
    if "avg_columns" in layer_summary.columns:
        layer_summary["avg_columns"] = layer_summary["avg_columns"].round(1)
    st.dataframe(layer_summary, use_container_width=True, hide_index=True)

    st.subheader("All Tables")
    display_cols = ["table_name", "layer", "row_count", "column_count",
                    "description", "business_owner", "scanned_at"]
    available = [c for c in display_cols if c in dd_tables.columns]
    display_df = dd_tables[available]
    st.dataframe(display_df, use_container_width=True, hide_index=True)

else:
    # Table detail view
    tbl_row = dd_tables[dd_tables["table_name"] == selected].iloc[0]
    layer = tbl_row["layer"]
    color = LAYER_COLORS.get(layer, "#94a3b8")

    # Header
    st.markdown(
        f'<span class="layer-badge" style="background:{color}20;color:{color}">'
        f'{layer.upper()}</span> <b style="font-size:24px">{selected}</b>',
        unsafe_allow_html=True,
    )

    # Quick stats (from dd_tables, updated by scan)
    s1, s2, s3 = st.columns(3)
    rc = _safe_get(tbl_row, "row_count")
    cc = _safe_get(tbl_row, "column_count")
    s1.metric("Rows", f"{int(rc):,}" if rc is not None else "?")
    s2.metric("Columns", int(cc) if cc is not None else "?")
    sa = _safe_get(tbl_row, "scanned_at")
    s3.metric("Last Scanned", str(sa)[:19] if sa is not None else "Never")

    st.divider()

    # Editable table metadata
    st.subheader("Table Metadata")

    # AI suggest for table
    if st.button("🤖 AI Suggest Table Info", key="ai_tbl"):
        try:
            with st.spinner("Asking Groq AI..."):
                dd_cols_for_ai = load_dd_columns(selected)
                suggestion = ai_suggest_table(selected, layer, dd_cols_for_ai)
                st.session_state[f"ai_tbl_{selected}"] = suggestion
        except Exception as e:
            st.error(f"AI error: {e}")

    # Apply AI suggestion if available
    ai_tbl = st.session_state.get(f"ai_tbl_{selected}", {})

    with st.form(f"table_meta_{selected}"):
        fc1, fc2 = st.columns(2)
        desc = fc1.text_area(
            "Description",
            value=ai_tbl.get("description") or _safe_get(tbl_row, "description", ""),
            height=80,
        )
        owner = fc2.text_input(
            "Business Owner",
            value=ai_tbl.get("business_owner") or _safe_get(tbl_row, "business_owner", ""),
        )
        fc3, fc4, fc5 = st.columns(3)
        source = fc3.text_input(
            "Source System",
            value=ai_tbl.get("source_system") or _safe_get(tbl_row, "source_system", ""),
        )
        freq_options = ["", "hourly", "daily", "weekly", "monthly", "ad-hoc", "real-time"]
        ai_freq = ai_tbl.get("refresh_frequency", "")
        cur_freq = ai_freq if ai_freq in freq_options else _safe_get(tbl_row, "refresh_frequency", "")
        freq = fc4.selectbox(
            "Refresh Frequency",
            freq_options,
            index=freq_options.index(cur_freq) if cur_freq in freq_options else 0,
        )
        tags = fc5.text_input(
            "Tags (comma-separated)",
            value=ai_tbl.get("tags") or _safe_get(tbl_row, "tags", ""),
        )

        if st.form_submit_button("💾 Save Table Info", type="primary"):
            update_table_fields(selected, {
                "description": desc or None,
                "business_owner": owner or None,
                "source_system": source or None,
                "refresh_frequency": freq or None,
                "tags": tags or None,
            })
            if _is_fabric():
                st.success("Table metadata queued! Check sidebar for generated code.")
            else:
                st.success("Table metadata saved!")
            st.rerun()

    st.divider()

    # Column grid
    st.subheader("Columns")
    # Load column stats on-demand (null%, distinct, samples) — only when viewing this table
    dd_cols = load_column_stats(st.session_state.env, selected)
    if _is_fabric() and not dd_cols.empty:
        dd_cols = _apply_column_overrides(dd_cols)

    if dd_cols.empty:
        # Fallback to basic dd_columns without stats
        dd_cols = load_dd_columns(selected)

    if dd_cols.empty:
        st.info("No column data. Run a scan first.")
    else:
        # AI suggest for columns
        if st.button("🤖 AI Suggest All Columns", key="ai_cols"):
            try:
                with st.spinner("Asking Groq AI..."):
                    suggestions = ai_suggest_columns(selected, layer, dd_cols)
                    st.session_state[f"ai_cols_{selected}"] = {
                        s["column_name"]: s for s in suggestions
                    }
            except Exception as e:
                st.error(f"AI error: {e}")

        # Apply AI column suggestions into display_df
        ai_cols = st.session_state.get(f"ai_cols_{selected}", {})

        # Prepare editable dataframe
        edit_cols = [
            "column_name", "data_type", "is_primary_key", "is_nullable",
            "null_percentage", "distinct_count", "sample_values",
            "description", "business_name",
        ]
        # original_df keeps the raw DB values for comparison
        original_df = dd_cols[[c for c in edit_cols if c in dd_cols.columns]].copy()
        display_df = original_df.copy()

        # Apply AI suggestions to empty fields (only in display_df, not original_df)
        if ai_cols:
            for idx, row in display_df.iterrows():
                cn = row.get("column_name", "")
                if cn in ai_cols:
                    s = ai_cols[cn]
                    if "description" in display_df.columns and pd.isna(row.get("description")):
                        display_df.at[idx, "description"] = s.get("description", "")
                    if "business_name" in display_df.columns and pd.isna(row.get("business_name")):
                        display_df.at[idx, "business_name"] = s.get("business_name", "")

        # Convert is_primary_key to bool for checkbox
        if "is_primary_key" in display_df.columns:
            display_df["is_primary_key"] = display_df["is_primary_key"].astype(bool)

        column_config = {
            "column_name": st.column_config.TextColumn("Column", disabled=True),
            "data_type": st.column_config.TextColumn("Type", disabled=True),
            "is_primary_key": st.column_config.CheckboxColumn("PK", width="small"),
            "is_nullable": st.column_config.TextColumn("Nullable", disabled=True, width="small"),
            "null_percentage": st.column_config.NumberColumn("Null %", format="%.1f", disabled=True),
            "distinct_count": st.column_config.NumberColumn("Distinct", disabled=True),
            "sample_values": st.column_config.TextColumn("Samples", disabled=True, width="large"),
            "description": st.column_config.TextColumn("Description", width="large"),
            "business_name": st.column_config.TextColumn("Business Name"),
        }

        edited = st.data_editor(
            display_df,
            column_config=column_config,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key=f"col_editor_{selected}",
        )

        if st.button("💾 Save Column Edits", type="primary"):
            changes = 0
            for idx in range(len(edited)):
                orig_row = original_df.iloc[idx]
                edit_row = edited.iloc[idx]
                col_name = orig_row["column_name"]
                updates = {}
                for field in ["description", "business_name", "is_primary_key"]:
                    if field in orig_row and field in edit_row:
                        ov = orig_row[field]
                        ev = edit_row[field]
                        # Normalize for comparison
                        if pd.isna(ov):
                            ov = None
                        if pd.isna(ev):
                            ev = None
                        if field == "is_primary_key":
                            ov = bool(ov) if ov is not None else False
                            ev = bool(ev) if ev is not None else False
                            if ov != ev:
                                updates[field] = 1 if ev else 0
                        elif str(ov or "") != str(ev or ""):
                            updates[field] = ev
                if updates:
                    update_column_fields(selected, col_name, updates)
                    changes += 1

            if changes:
                if _is_fabric():
                    st.success(f"Queued {changes} column(s)! Check sidebar for generated code.")
                else:
                    st.success(f"Saved {changes} column(s)!")
                st.rerun()
            else:
                st.info("No changes detected.")