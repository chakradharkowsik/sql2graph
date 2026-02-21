# enrich_registry.py
import json
import os
import re
import sqlite3
from typing import List


def normalize_token(s: str) -> str:
    return re.sub(r'[^0-9a-z]', '_', s.lower()).strip('_')

def split_col_tokens(col: str) -> List[str]:
    # split snake/camel and remove common suffixes
    s = re.sub(r'(_id|_no|id|no)$', '', col, flags=re.I)
    parts = re.split(r'[_\s]+', s)
    return [normalize_token(p) for p in parts if p]

def make_templates(table: str, top_cols: List[str]) -> List[str]:
    # choose likely pk and date columns heuristically
    pk = top_cols[0] if top_cols else "id"
    date = next((c for c in top_cols if 'date' in c.lower()), None)
    insurer_col = next((c for c in top_cols if 'insur' in c.lower() or 'company' in c.lower()), None)

    templates = []
    templates.append(f"Get {table} by {pk}")
    if date:
        templates.append(f"List {table} where {date} between {{start_date}} and {{end_date}}")
    else:
        templates.append(f"List {table} for insurer {{insurer_name}}")
    # generic detail query
    templates.append(f"Show details for {table} for Hindustan Petroleum")
    return templates

def enrich_registry(ndjson_path, sqlite_path, schema_path)->(str,str,str):
    backup_dir = os.path.join(os.getcwd(),"backup")
    os.makedirs(backup_dir, exist_ok=True)
    BACKUP = os.path.join(backup_dir,"registry.ndjson.bak")
    
    # load schema (optional)
    schema = {}
    if os.path.exists(schema_path):
        with open(schema_path, 'r', encoding='utf8') as f:
            schema = json.load(f)

    # backup ndjson
    if not os.path.exists(ndjson_path):
        raise FileNotFoundError(f"Registry file not found: {ndjson_path}")
    
    # Create backup by moving the original file
    os.replace(ndjson_path, BACKUP)

    # open sqlite
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    # read backup and write new ndjson
    with open(BACKUP, 'r', encoding='utf8') as fin, open(ndjson_path, 'w', encoding='utf8') as fout:
        for line in fin:
            doc = json.loads(line)
            table = doc.get("table")
            top_cols = doc.get("top_columns", [])
            # if sample_queries empty, generate templates
            if not doc.get("sample_queries"):
                doc["sample_queries"] = make_templates(table, top_cols)
            # generate simple synonyms from table name and top columns
            aliases = set(doc.get("aliases", []))
            aliases.add(normalize_token(table))
            for part in table.split('_'):
                aliases.add(normalize_token(part))
            for c in top_cols:
                for tok in split_col_tokens(c):
                    aliases.add(tok)
            # add insurer/company tokens heuristically
            if any('insur' in c.lower() for c in top_cols):
                aliases.add('insurer')
                aliases.add('insurance')
            doc["aliases"] = list(sorted(aliases))

            # write updated doc
            fout.write(json.dumps(doc, ensure_ascii=False) + "\n")

            # update sqlite alias_index for new aliases (idempotent insert)
            for a in doc["aliases"]:
                try:
                    cur.execute("INSERT INTO alias_index(token, table_name) VALUES (?,?)", (a, table))
                except Exception:
                    pass
            # also insert column tokens
            for c in schema.get(table, top_cols):
                for tok in split_col_tokens(c):
                    try:
                        cur.execute("INSERT INTO column_index(token, table_name, column_name) VALUES (?,?,?)", (tok, table, c))
                    except Exception:
                        pass

    conn.commit()
    conn.close()
    print("Enrichment complete. Wrote:", ndjson_path)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--ndjson_path", required=True, help="Path to registry.ndjson")
    p.add_argument("--sqlite_path", required=True, help="Path to registry.db")
    p.add_argument("--schema_path", required=True, help="Path to schema.json")
    args = p.parse_args()
    ndjson_path = args.ndjson_path
    sqlite_path = args.sqlite_path
    schema_path = args.schema_path
    print(f"ndjson_path {ndjson_path} and it exists: {os.path.exists(ndjson_path)}")
    print(f"sqlite_path {sqlite_path} and it exists : {os.path.exists(sqlite_path)}")
    print(f"schema_path {schema_path} and it exists : {os.path.exists(schema_path)}")
    enrich_registry(ndjson_path,sqlite_path,schema_path)
