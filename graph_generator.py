#!/usr/bin/env python3
"""
run_pipeline.py

Usage:
  python run_pipeline.py input_dump.sql --out out_folder [--ddl-script path] [--registry-script path]

This runs:
  1) python ddl_to_schema.py input_dump.sql schema.json
  2) python build_registry.py schema.json --out out_folder
  3) If above runs correctly and produces output files then call enrich_registry.py registry.ndjson, registry.db, schema.json

Exits non-zero on any step failure.
"""
import argparse
import os
import subprocess
import sys


def run(cmd, cwd=None):
    print("RUN:", " ".join(cmd))
    res = subprocess.run(cmd, cwd=cwd)
    if res.returncode != 0:
        print("Command failed:", " ".join(cmd), "exitcode=", res.returncode)
        sys.exit(res.returncode)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("sql_file", help="Input .sql dump")
    p.add_argument("--out-dir", required=True, help="Output folder for registry artifacts")
    p.add_argument("--ddl-script", default=os.path.join(os.getcwd(),"utils","ddl_to_schema.py"), help="Path to ddl_to_schema.py")
    p.add_argument("--registry-script", default=os.path.join(os.getcwd(),"utils","build_registry.py"), help="path to build_registry.py")
    p.add_argument("--schema-json", default=os.path.join(os.getcwd(),"schema.json"), help="Temporary schema.json path (will be overwritten)")
    p.add_argument("--enrich-script",default=os.path.join(os.getcwd(),"utils","enrich_registry.py"),help="Path to enrich the registry.ndjson")
    args = p.parse_args()

    sql_file = os.path.join(os.getcwd(), args.sql_file)
    out_dir = os.path.join(os.getcwd(), args.out_dir)
    ddl_script = args.ddl_script
    registry_script = args.registry_script
    enrich_registry = args.enrich_script
    schema_json = os.path.join(os.getcwd(), out_dir, "schema.json")

    os.makedirs(out_dir, exist_ok=True)
    print(f"Running ddl_to_schema.py from {ddl_script}")
    # Step 1: run ddl_to_schema.py -> schema.json
    run([sys.executable, ddl_script, sql_file, schema_json])

    print(f"Running build_registry.py from {registry_script}")
    # Step 2: run build_registry.py -> registry files in out_dir
    run([sys.executable, registry_script, schema_json, "--out-dir", out_dir])

    print("Pipeline completed. Outputs saved to:", out_dir)
    print("Files produced (example):")
    files_generated = True
    for fname in ("schema.json","registry.ndjson","registry.db"):
        path = os.path.join(out_dir, fname)
        if not os.path.exists(path):
            print("  -", path, "not found")
            files_generated = False

    if  files_generated:
        print("Enriching the registry.ndjson to have some sample queries")
        schema_file = os.path.join(out_dir, "schema.json")
        ndjson_path = os.path.join(out_dir,"registry.ndjson")
        registry_db_path = os.path.join(out_dir,"registry.db")
         # Step 3: run enrich_registry.py -> enriches registry.ndjson file in out_dir
        run([sys.executable,enrich_registry, "--ndjson_path" ,ndjson_path,"--sqlite_path",registry_db_path,"--schema_path",schema_file])


if __name__ == "__main__":
    main()
