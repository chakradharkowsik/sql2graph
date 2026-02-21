# streamlit_enrich.py
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Dict, List

import streamlit as st

# ---------- Configuration ----------
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
OUT_DIR = os.getenv("REGISTRY_OUT_DIR", str(PROJECT_ROOT / "out"))

# ---------- Utility functions ----------
def normalize_token(s: str) -> str:
    return re.sub(r'[^0-9a-z]', '_', s.lower()).strip('_')

def tokenize_text(s: str) -> List[str]:
    return list({t for t in re.sub(r'[^0-9a-z]', ' ', s.lower()).split() if t})

def sanitize_text(s: str, maxlen: int = 200) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = re.sub(r'<[^>]+>', '', s)            # remove angle-bracket tags
    s = re.sub(r'\s+', ' ', s).strip()       # collapse whitespace
    return s[:maxlen]

def get_paths(out_dir: str = None):
    if out_dir is None:
        out_dir = OUT_DIR
    return {
        'ndjson': os.path.join(out_dir, "registry.ndjson"),
        'db': os.path.join(out_dir, "registry.db"),
        'schema': os.path.join(out_dir, "schema.json"),
    }

# ---------- File I/O (cached) ----------
@st.cache_data
def load_docs_cached(path: str) -> List[Dict]:
    docs = []
    if not os.path.exists(path):
        return docs
    with open(path, 'r', encoding='utf8') as f:
        for line in f:
            if line.strip():
                try:
                    docs.append(json.loads(line))
                except Exception:
                    # skip malformed lines
                    continue
    return docs

@st.cache_data
def load_schema_cached(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    with open(path, 'r', encoding='utf8') as f:
        try:
            return json.load(f)
        except Exception:
            return {}

def save_docs(path: str, docs: List[Dict]):
    tmp = path + ".tmp"
    with open(tmp, 'w', encoding='utf8') as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
    os.replace(tmp, path)

def upsert_sample_query(ndjson_path: str, sqlite_path: str, table: str, sample_obj):
    docs = load_docs_cached(ndjson_path)
    # convert cached list to mutable copy
    docs = list(docs)
    found = False
    for d in docs:
        if d.get("table") == table:
            found = True
            sq = d.get("sample_queries") or []
            if sample_obj not in sq:
                sq.append(sample_obj)
            d["sample_queries"] = sq
            aliases = set(d.get("aliases", []))
            aliases.add(normalize_token(table))
            d["aliases"] = list(sorted(aliases))
            break
    if not found:
        docs.append({
            "table": table,
            "sig": f"tbl:{table}|h:manual",
            "top_columns": [],
            "aliases": [normalize_token(table)],
            "sample_queries": [sample_obj],
            "neighbors": [],
            "sensitivity": "low"
        })
    # backup and save
    if os.path.exists(ndjson_path):
        os.replace(ndjson_path, ndjson_path + ".bak")
    save_docs(ndjson_path, docs)

    # update sqlite indexes
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    qtext = sample_obj['query'] if isinstance(sample_obj, dict) else str(sample_obj)
    for t in tokenize_text(qtext):
        try:
            cur.execute("INSERT INTO alias_index(token, table_name) VALUES (?,?)", (t, table))
        except Exception:
            pass
    if isinstance(sample_obj, dict):
        for j in sample_obj.get("joins", []):
            jt = j.get("table")
            if jt:
                for t in tokenize_text(jt):
                    try:
                        cur.execute("INSERT INTO alias_index(token, table_name) VALUES (?,?)", (t, jt))
                    except Exception:
                        pass
            on = j.get("on", "")
            for tok in re.findall(r'[A-Za-z0-9_]+\.[A-Za-z0-9_]+', on):
                tbl, col = tok.split('.', 1)
                try:
                    cur.execute("INSERT INTO column_index(token, table_name, column_name) VALUES (?,?,?)",
                                (normalize_token(col), tbl, col))
                except Exception:
                    pass
    conn.commit()
    conn.close()
    # clear caches so UI reloads updated content
    load_docs_cached.clear()
    load_schema_cached.clear()

# ---------- Path helpers and session init ----------
from pathlib import Path as _Path

def make_paths(out_dir: str):
    p = _Path(out_dir)
    return {
        "ndjson": str(p / "registry.ndjson"),
        "db": str(p / "registry.db"),
        "schema": str(p / "schema.json")
    }

def check_paths_exist(paths: dict) -> dict:
    return {k: _Path(v).exists() for k, v in paths.items()}

if "out_dir" not in st.session_state:
    st.session_state.out_dir = OUT_DIR
if "exists_map" not in st.session_state:
    st.session_state.exists_map = check_paths_exist(make_paths(st.session_state.out_dir))
if "paths_ok" not in st.session_state:
    st.session_state.paths_ok = all(st.session_state.exists_map.values())

# ---------- Streamlit UI ----------
try:
    st.set_page_config(page_title="Registry Enricher", layout="wide")
except Exception:
    pass

st.title("Registry Enricher")

# Sidebar: configuration (unique widget keys)
with st.sidebar:
    st.subheader("Configuration")
    custom_out_dir = st.text_input("Output directory", value=st.session_state.get('out_dir', OUT_DIR),
                                  help="Directory containing registry files", key="out_dir_sidebar")
    if custom_out_dir and custom_out_dir.strip():
        current_out_dir = os.path.abspath(custom_out_dir.strip())
        st.session_state['out_dir'] = current_out_dir
    else:
        current_out_dir = st.session_state.get('out_dir', OUT_DIR)

    paths = get_paths(current_out_dir)
    st.write(f"**Registry NDJSON:** `{paths['ndjson']}`")
    st.write(f"**Registry DB:** `{paths['db']}`")
    st.write(f"**Schema JSON:** `{paths['schema']}`")

    if not os.path.exists(paths['ndjson']):
        st.error("⚠ Registry file not found!")
        st.info(f"Run: `python sql_to_registry.py <sql_file> --out-dir {current_out_dir}`")

# Use session out_dir and compute paths
current_out_dir = st.session_state.get('out_dir', OUT_DIR)
paths = get_paths(current_out_dir)
NDJSON = paths['ndjson']
DB = paths['db']
SCHEMA = paths['schema']

# Load docs and schema (cached)
docs = load_docs_cached(NDJSON)
tables = [d['table'] for d in docs]
schema = load_schema_cached(SCHEMA)

# Out Dir + Refresh UI (main area)
st.subheader("Artifacts directory and file check")
st.session_state.out_dir = st.text_input("Output directory", value=st.session_state.get('out_dir', OUT_DIR),
                                        help="Directory containing registry files", key="out_dir_main")
paths = make_paths(st.session_state.out_dir)
if st.button("Refresh Paths"):
    st.session_state.exists_map = check_paths_exist(paths)
    st.session_state.paths_ok = all(st.session_state.exists_map.values())

exists_map = st.session_state.get("exists_map", check_paths_exist(paths))
st.write("**File status:**")
c1, c2, c3 = st.columns(3)
if exists_map.get("ndjson"):
    c1.success("registry.ndjson — Exists")
else:
    c1.error("registry.ndjson — Missing")
if exists_map.get("db"):
    c2.success("registry.db — Exists")
else:
    c2.error("registry.db — Missing")
if exists_map.get("schema"):
    c3.success("schema.json — Exists")
else:
    c3.error("schema.json — Missing")
st.session_state.paths_ok = all(exists_map.values())

# Main editor area
col1, col2 = st.columns([2, 3])

with col1:
    st.subheader("Select Table")
    # searchable selection to handle many tables
    search_q = st.text_input("Find table (type substring)", key="table_search")
    if search_q:
        candidates = [t for t in tables if search_q.lower() in t.lower()][:200]
    else:
        candidates = sorted(tables)[:200]
    table = st.selectbox("Table", options=candidates or ["<no tables found>"], key="table_select")
    st.markdown("Or enter a new table name to create a minimal doc")
    new_table = st.text_input("New table name", value="", key="new_table_input")
    if new_table:
        table = new_table.strip()

    st.subheader("Sample Query")
    query_text = st.text_area("Natural language query", height=80,
                              placeholder="e.g., List policies due next month for Zurich Insurance",
                              key="query_text")
    st.text_input("Intent tag", key="intent_input")
    st.number_input("Confidence 0.0 to 1.0", min_value=0.0, max_value=1.0, value=0.9, step=0.1, key="confidence_input")
    st.markdown("Add joins. Use Add Row to add multiple join hints.")

    # data editor with fallback; unique key used
    try:
        if hasattr(st, 'data_editor'):
            joins = st.data_editor([{"table": "", "on": ""}], num_rows="dynamic", key="joins_editor_v1")
        else:
            joins = st.experimental_data_editor([{"table": "", "on": ""}], num_rows="dynamic", key="joins_editor_v2")
    except Exception:
        st.warning("Data editor not available. Using simple text input.")
        join_text = st.text_area("Joins (JSON format)", value='[{"table":"","on":""}]', key="joins_text")
        try:
            joins = json.loads(join_text)
        except json.JSONDecodeError:
            joins = [{"table": "", "on": ""}]

    if st.button("Preview Sample Object", key="preview_btn"):
        sample = {"query": sanitize_text(query_text)}
        intent_val = st.session_state.get("intent_input")
        if intent_val:
            sample["intent"] = intent_val
        sample["confidence"] = float(st.session_state.get("confidence_input", 0.9))
        sample["joins"] = [j for j in joins if j.get("table") and j.get("on")]
        st.json(sample)

    # Guarded Save: disabled unless required files exist
    save_disabled = not st.session_state.get("paths_ok", False)
    if save_disabled:
        st.warning("Cannot save: one or more artifact files are missing. Click Refresh Paths after fixing.")
    if st.button("Save Sample Query", disabled=save_disabled, key="save_btn"):
        if not table or not query_text.strip():
            st.error("Table and query are required")
        else:
            sample = {"query": sanitize_text(query_text.strip())}
            intent_val = st.session_state.get("intent_input")
            if intent_val:
                sample["intent"] = intent_val
            sample["confidence"] = float(st.session_state.get("confidence_input", 0.9))
            sample["joins"] = [j for j in joins if j.get("table") and j.get("on")]
            ndjson_path = make_paths(st.session_state.out_dir)["ndjson"]
            db_path = make_paths(st.session_state.out_dir)["db"]
            upsert_sample_query(ndjson_path, db_path, table, sample)
            st.session_state.exists_map = check_paths_exist(make_paths(st.session_state.out_dir))
            st.session_state.paths_ok = all(st.session_state.exists_map.values())
            st.success(f"Saved sample query for {table}")

with col2:
    st.subheader("Table Doc Preview")
    if table and docs:
        doc = next((d for d in docs if d['table'] == table), None)
        if doc:
            # show a trimmed preview to avoid heavy rendering
            preview = {
                "table": doc.get("table"),
                "top_columns": doc.get("top_columns", [])[:10],
                "aliases": doc.get("aliases", [])[:20],
                "sample_queries": doc.get("sample_queries", [])[:5],
                "neighbors": doc.get("neighbors", [])[:5],
                "sensitivity": doc.get("sensitivity", "low")
            }
            st.json(preview)
        else:
            st.info("Table not found in registry. It will be created on save.")
    st.subheader("Schema Quick View")
    if table and table in schema:
        st.write("Columns (preview):", schema[table][:50])
    else:
        st.write("No schema available for this table or schema.json missing.")

st.markdown("---")
st.write("Operational notes")
st.write("- Changes update registry.ndjson and registry.db. Keep backups and use git for review.")
st.write("- The app inserts simple tokens into alias_index and column_index to improve rule matching.")
