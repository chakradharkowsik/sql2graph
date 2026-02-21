#!/usr/bin/env python3
"""
sql_to_registry.py - Pipeline to convert SQL DDL files to registry format

Usage:
  python sql_to_registry.py input_dump.sql --out-dir out_folder [--validate]

This runs a three-step pipeline:
  1) ddl_to_schema.py: Extracts CREATE TABLE statements and generates schema.json
  2) build_registry.py: Creates registry.ndjson and registry.db with table metadata and indexes
  3) enrich_registry.py: Adds sample queries and additional aliases to the registry

Optional:
  --validate: Test that the generated files can be loaded by TableRegistry

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
    p = argparse.ArgumentParser(description="Convert SQL DDL to registry format for LangChain integration")
    p.add_argument("sql_file", help="Input .sql dump file")
    p.add_argument("--out-dir", required=True, help="Output folder for registry artifacts")
    p.add_argument("--ddl-script", default=os.path.join(os.getcwd(),"utils","ddl_to_schema.py"), help="Path to ddl_to_schema.py")
    p.add_argument("--registry-script", default=os.path.join(os.getcwd(),"utils","build_registry.py"), help="Path to build_registry.py")
    p.add_argument("--schema-json", default=os.path.join(os.getcwd(),"schema.json"), help="Temporary schema.json path (will be overwritten)")
    p.add_argument("--enrich-script", default=os.path.join(os.getcwd(),"utils","enrich_registry.py"), help="Path to enrich_registry.py")
    p.add_argument("--validate", action="store_true", help="Validate generated files by testing TableRegistry loading")
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

    if files_generated:
        print("Enriching the registry.ndjson to have some sample queries")
        schema_file = os.path.join(out_dir, "schema.json")
        ndjson_path = os.path.join(out_dir, "registry.ndjson")
        registry_db_path = os.path.join(out_dir, "registry.db")
        # Step 3: run enrich_registry.py -> enriches registry.ndjson file in out_dir
        run([sys.executable, enrich_registry, "--ndjson_path", ndjson_path, "--sqlite_path", registry_db_path, "--schema_path", schema_file])
        
        # Optional validation step
        if args.validate:
            print("\nValidating generated files...")
            try:
                # Import here to avoid requiring it for basic pipeline
                from utils.table_selector import TableRegistry
                
                registry = TableRegistry(ndjson_path, registry_db_path, schema_path=schema_file)
                table_count = len(registry.tables)
                print(f"✓ Successfully loaded registry with {table_count} tables")
                
                # Test a simple query
                test_tables = registry.find_tables_by_token("policy", limit=1)
                if test_tables:
                    print(f"✓ Test query successful: found {len(test_tables)} table(s) for token 'policy'")
                else:
                    print("⚠ Test query returned no results (this may be normal if no matching tables exist)")
                
                registry.close()
                print("✓ Validation passed!")
            except ImportError as e:
                print(f"⚠ Validation skipped: Could not import table_selector ({e})")
            except Exception as e:
                print(f"✗ Validation failed: {e}")
                # Don't fail the pipeline on validation errors
    else:
        print("⚠ Some files were not generated. Skipping enrichment step.")
        sys.exit(1)


if __name__ == "__main__":
    main()
