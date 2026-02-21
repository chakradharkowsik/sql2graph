#!/usr/bin/env python3
"""
ddl_to_schema_robust.py
Produce a simple schema.json mapping table -> [column,...] from a SQL dump.

Usage:
    python ddl_to_schema_robust.py input_dump.sql schema.json
"""

import json
import re
import sys
from typing import Dict, List

import sqlparse

# Helpers ---------------------------------------------------------
def strip_brackets_and_quotes(name: str) -> str:
    """
    Remove surrounding brackets/quotes and optional schema prefix.
    Tolerant: will remove unmatched leading/trailing bracket or quote.
    """
    if not name:
        return ""
    s = name.strip()

    # remove schema prefix if present (keep last part)
    if '.' in s:
        s = s.split('.')[-1].strip()

    # strip surrounding brackets or quotes if present, or lone leading/trailing ones
    if s.startswith('[') and s.endswith(']'):
        s = s[1:-1]
    else:
        # remove a single leading '[' or trailing ']' if unmatched
        if s.startswith('['):
            s = s[1:]
        if s.endswith(']'):
            s = s[:-1]

    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    else:
        # remove unmatched leading/trailing quotes
        if s.startswith('"') or s.startswith("'"):
            s = s[1:]
        if s.endswith('"') or s.endswith("'"):
            s = s[:-1]

    return s.strip()

def find_create_table_blocks(sql_text: str) -> List[Dict[str,str]]:
    """
    Return list of dicts: {"table": raw_table_identifier, "cols_block": text_inside_parentheses}
    Uses a scanning approach to find 'CREATE TABLE' and the matching parentheses block.
    """
    blocks = []
    # Normalize whitespace for keyword search but keep original for extraction
    lower = sql_text.lower()
    idx = 0
    while True:
        pos = lower.find('create table', idx)
        if pos == -1:
            break
        # move to the original position in sql_text
        start = pos
        # find the opening parenthesis after the table name
        # we search forward from pos to find the first '(' that starts the column list
        open_paren_pos = sql_text.find('(', pos)
        if open_paren_pos == -1:
            idx = pos + 12
            continue
        # backtrack from open_paren_pos to capture the table identifier
        pre = sql_text[pos:open_paren_pos]
        # try to extract the table name from 'CREATE TABLE <name>' (allowing optional schema and brackets)
        m = re.search(r'create\s+table\s+([A-Za-z0-9_\.[\[\]"]+)\s*$', pre, flags=re.I)
        if not m:
            # fallback: take the token immediately before '('
            token = pre.strip().split()[-1] if pre.strip().split() else None
            raw_table = token or "unknown_table"
        else:
            raw_table = m.group(1)
        # now scan forward to find the matching closing parenthesis, handling nested parentheses
        i = open_paren_pos
        depth = 0
        end_pos = None
        L = len(sql_text)
        while i < L:
            ch = sql_text[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    end_pos = i
                    break
            i += 1
        if end_pos is None:
            # no matching close paren found; skip this occurrence
            idx = pos + 12
            continue
        cols_block = sql_text[open_paren_pos+1:end_pos]
        blocks.append({"table_raw": raw_table, "cols_block": cols_block})
        idx = end_pos + 1
    return blocks

def split_top_level_commas(s: str) -> List[str]:
    """
    Split string s by commas that are at top-level (not inside parentheses or brackets or quotes).
    Returns list of parts (trimmed).
    """
    parts = []
    cur = []
    depth = 0
    in_sq = False
    in_dq = False
    in_br = False
    i = 0
    while i < len(s):
        ch = s[i]
        # handle escapes inside quotes for safety
        if ch == "'" and not in_dq and not in_br:
            in_sq = not in_sq
            cur.append(ch)
        elif ch == '"' and not in_sq and not in_br:
            in_dq = not in_dq
            cur.append(ch)
        elif ch == '[' and not in_sq and not in_dq:
            in_br = True
            cur.append(ch)
        elif ch == ']' and in_br:
            in_br = False
            cur.append(ch)
        elif in_sq or in_dq or in_br:
            cur.append(ch)
        else:
            if ch == '(':
                depth += 1
                cur.append(ch)
            elif ch == ')':
                if depth > 0:
                    depth -= 1
                cur.append(ch)
            elif ch == ',' and depth == 0:
                part = ''.join(cur).strip()
                if part:
                    parts.append(part)
                cur = []
            else:
                cur.append(ch)
        i += 1
    last = ''.join(cur).strip()
    if last:
        parts.append(last)
    return parts

def parse_column_name_from_def(col_def: str) -> str:
    """
    Given a column definition line, attempt to extract the column name.
    Handles:
      - [colname] datatype ...
      - "colname" datatype ...
      - colname datatype ...
      - CONSTRAINT / PRIMARY KEY lines return empty string
    """
    s = col_def.strip()
    # ignore table-level constraints
    if re.match(r'^(constraint|primary\s+key|unique|foreign\s+key|check|index|alter|constraint)', s, flags=re.I):
        return ""
    # remove trailing commas if any
    if s.endswith(','):
        s = s[:-1].strip()
    # column name is the first token, but may be bracketed or quoted
    # match bracketed or quoted or bare identifier
    m = re.match(r'^\s*(?:\[(?P<b>[^\]]+)\]|"(?P<q>[^"]+)"|`(?P<bt>[^`]+)`|(?P<id>[A-Za-z0-9_]+))', s)

    if not m:
        # fallback: split by whitespace
        toks = s.split()
        return strip_brackets_and_quotes(toks[0]) if toks else ""
    name = m.group('b') or m.group('q') or m.group('bt') or m.group('id')
    return strip_brackets_and_quotes(name)

# Main ------------------------------------------------------------
def ddl_to_schema(sql_text: str) -> Dict[str, List[str]]:
    tables = {}
    blocks = find_create_table_blocks(sql_text)
    for blk in blocks:
        raw_table = blk['table_raw']
        table = strip_brackets_and_quotes(raw_table)
        cols_block = blk['cols_block']
        # split into top-level comma-separated column/constraint definitions
        parts = split_top_level_commas(cols_block)
        cols = []
        for p in parts:
            colname = parse_column_name_from_def(p)
            if colname:
                if colname not in cols:
                    cols.append(colname)
        if cols:
            tables[table] = cols
    return tables

# CLI -------------------------------------------------------------
def main():

    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("in_file", help="Path to input .sql dump")
    p.add_argument("out_file", help="Path to output schema.json")
    args = p.parse_args()
    in_file = args.in_file
    out_file = args.out_file
    print(f"Reading {in_file} and writing to {out_file}")
    with open(in_file, 'r', encoding='utf-8', errors='ignore') as f:
        sql_text = f.read()
    # optional: remove common noise like GO lines to simplify scanning
    # but keep them for sqlparse; we already handle GO in scanning
    schema = ddl_to_schema(sql_text)
    print(f"Writing {len(schema)} tables to {out_file}")
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(schema)} tables to {out_file}")

if __name__ == "__main__":
    main()
