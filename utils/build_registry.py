# build_registry.py
from collections import defaultdict
import hashlib
import json
import os
import re
import sqlite3

def normalize(s):
    return re.sub(r'[^0-9a-z]', '_', s.lower())

def signature(table, cols):
    base = table + '|' + ','.join(cols)
    return hashlib.sha1(base.encode()).hexdigest()[:8]

def pick_top_columns(cols, n=4):
    # heuristic: prefer *_id, *_no, date, name, status
    priority = lambda c: (c.endswith('_id') or c.endswith('id'), 'date' in c, 'name' in c, 'status' in c)
    cols_sorted = sorted(cols, key=lambda c: priority(c), reverse=True)
    return cols_sorted[:n]

def build_registry(schema_dict, out_file='registry.ndjson', sqlite_file='registry.db'):
    # schema_dict: {table: [col1,col2,...]}
    conn = sqlite3.connect(sqlite_file)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS alias_index(token TEXT, table_name TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS column_index(token TEXT, table_name TEXT, column_name TEXT)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_alias ON alias_index(token)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_col ON column_index(token)")
    with open(out_file, 'w', encoding='utf8') as f:
        for table, cols in schema_dict.items():
            top = pick_top_columns(cols, n=4)
            sig = signature(table, top)
            aliases = list({normalize(table)} | {normalize(t) for t in table.split('_')})
            doc = {
                "table": table,
                "sig": f"tbl:{table}|h:{sig}",
                "top_columns": top,
                "aliases": aliases,
                "sample_queries": [],
                "neighbors": [], "sensitivity":"low"
            }
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            # populate sqlite indexes
            for a in aliases:
                cur.execute("INSERT INTO alias_index(token, table_name) VALUES (?,?)", (a, table))
            for c in cols:
                for t in set(re.split(r'[_\s]+', normalize(c))):
                    cur.execute("INSERT INTO column_index(token, table_name, column_name) VALUES (?,?,?)", (t, table, c))
    conn.commit()
    conn.close()

# Example usage:
if __name__ == '__main__':
    # load schema from a simple JSON: {"policies":["pol_policyid","pol_policyno",...], ...}
    import sys
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("schema_file", help="Path to schema.json")
    p.add_argument("--out-dir", required=True, help="Output folder for registry artifacts")
    args = p.parse_args()
    schema_file = args.schema_file
    out_dir = args.out_dir
    print(f"Building registry from {schema_file} to {out_dir}")
    print(f"the schema file exists: {os.path.exists(schema_file)}")
    print(f"the out_dir exists: {os.path.exists(out_dir)}")
    schema = json.load(open(schema_file))
    build_registry(schema, out_file=os.path.join(out_dir, "registry.ndjson"), sqlite_file=os.path.join(out_dir, "registry.db"))
    print(f"Registry built and saved to {os.path.join(out_dir, "registry.ndjson")}")
    print(f"Registry built and saved to {os.path.join(out_dir, "registry.db")}")
