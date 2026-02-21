# SQL2Graph

A Python tool to convert SQL DDL schemas into structured graph-based registries for natural language query processing. This tool extracts table and column information from SQL DDL files and creates searchable indexes that can be used with LangChain and other AI frameworks for intelligent table selection and query generation.

## Features

- **DDL Parsing**: Extracts table and column information from SQL CREATE TABLE statements
- **Token Indexing**: Creates fast SQLite-based indexes for table and column lookups
- **Metadata Enrichment**: Generates aliases, top columns, and sample queries for each table
- **Interactive UI**: Streamlit web interface for manually enriching registry with sample queries and join hints
- **LangChain Ready**: Provides easy-to-use classes and tools for integration with LangChain agents

## Overview

SQL2Graph processes SQL DDL files and generates three key artifacts:

1. **`schema.json`** - A JSON mapping of table names to their column lists
2. **`registry.ndjson`** - A newline-delimited JSON file containing enriched table metadata (aliases, top columns, sample queries, etc.)
3. **`registry.db`** - A SQLite database with token-based indexes for fast table and column lookups

## Installation

```bash
# Install dependencies (using uv or pip)
uv sync
# or
pip install -r requirements.txt
```

## Quick Start

### Generate Registry Files

Run the pipeline to convert a SQL DDL file into the registry format:

```bash
python sql_to_registry.py dbscripts/sample_sql_schema.sql --out-dir out
```

To validate the generated files can be loaded correctly, add the `--validate` flag:

```bash
python sql_to_registry.py dbscripts/sample_sql_schema.sql --out-dir out --validate
```

This will generate three files in the `out/` directory:
- `out/schema.json` - Table to columns mapping
- `out/registry.ndjson` - Enriched table registry (one JSON object per line)
- `out/registry.db` - SQLite database with token indexes

> **Tip**: The `out/` directory will be created automatically if it doesn't exist.

### Output File Structure

**schema.json** format:
```json
{
  "policies": ["pol_policyid", "pol_policyno", "pol_customerid", "pol_insurerid"],
  "customer": ["cust_id", "cust_name", "cust_type", "cust_status_key"]
}
```

**registry.ndjson** format (each line is a JSON object):
```json
{"table": "policies", "sig": "tbl:policies|h:abc12345", "top_columns": ["pol_policyid", "pol_policyno"], "aliases": ["policies", "policy"], "sample_queries": ["Get policies by pol_policyid"], "neighbors": [], "sensitivity": "low"}
```

**registry.db** contains:
- `alias_index` table: token → table_name mappings
- `column_index` table: token → (table_name, column_name) mappings

## LangChain Integration Examples

The following examples demonstrate how to use the generated output files (`schema.json`, `registry.ndjson`, and `registry.db`) with LangChain.

The `TableRegistry` and `TableSelectorTool` classes are available in `utils/table_selector.py`.

### Example 1: Basic Usage

Simple example of loading the registry and creating a LangChain tool:

```python
from langchain.tools import Tool
from utils.table_selector import TableRegistry, TableSelectorTool

# Point to your generated artifacts
REGISTRY_NDJSON = "out/registry.ndjson"
REGISTRY_DB = "out/registry.db"
SCHEMA_JSON = "out/schema.json"

# Load the registry and create the tool
registry = TableRegistry(REGISTRY_NDJSON, REGISTRY_DB, schema_path=SCHEMA_JSON)
selector = TableSelectorTool(registry)

table_selector_tool = Tool(
    name="table_selector",
    func=selector._run,
    description="Return top candidate tables and top columns for a natural language database query."
)

# Example usage
if __name__ == "__main__":
    q1 = "give me details of Hindustan Petroleum"
    q2 = "what are the policies due in next month for Zurich Insurance"
    
    print("Query 1 result:", table_selector_tool.func(q1))
    print("Query 2 result:", table_selector_tool.func(q2))
    
    registry.close()
```

### Example 2: Using with LangChain Agents

Integrate the table selector into a LangChain agent:

```python
from langchain.agents import initialize_agent, AgentType
from langchain.llms import OpenAI  # or use ChatOpenAI for newer versions
from langchain.tools import Tool
from utils.table_selector import TableRegistry, TableSelectorTool

# Initialize registry and tool
registry = TableRegistry("out/registry.ndjson", "out/registry.db", "out/schema.json")
selector = TableSelectorTool(registry)

table_selector_tool = Tool(
    name="table_selector",
    func=selector._run,
    description="Find relevant database tables and columns for a natural language query. Input: user's question about data."
)

# Create agent with the tool
llm = OpenAI(temperature=0)
tools = [table_selector_tool]

agent = initialize_agent(
    tools,
    llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True
)

# Use the agent
response = agent.run("What tables should I query to find customer policy information?")
print(response)
```

### Example 3: Advanced Query with Schema Context

Use the `schema.json` to provide full column context using the `SchemaAwareTool`:

```python
from langchain.tools import Tool
from utils.table_selector import TableRegistry, SchemaAwareTool

# Load registry with schema
registry = TableRegistry("out/registry.ndjson", "out/registry.db", "out/schema.json")
schema_tool = SchemaAwareTool(registry)

tool = Tool(
    name="schema_aware_selector",
    func=schema_tool._run,
    description="Find database tables with full schema information for query generation."
)

# Use the tool
result = tool.func("customer insurance policies")
print(result)
```

### Example 4: Direct Registry Query

Query the registry files directly without LangChain:

```python
import json
import sqlite3

# Load registry.ndjson
def load_registry(ndjson_path: str):
    tables = {}
    with open(ndjson_path, 'r', encoding='utf8') as f:
        for line in f:
            doc = json.loads(line)
            tables[doc['table']] = doc
    return tables

# Query SQLite index
def search_tables(db_path: str, search_term: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    token = search_term.lower().replace(' ', '_')
    
    # Find tables
    cur.execute(
        "SELECT DISTINCT table_name FROM alias_index WHERE token LIKE ?",
        (f'%{token}%',)
    )
    tables = [row[0] for row in cur.fetchall()]
    
    # Find columns
    cur.execute(
        "SELECT DISTINCT table_name, column_name FROM column_index WHERE token LIKE ?",
        (f'%{token}%',)
    )
    columns = [(row[0], row[1]) for row in cur.fetchall()]
    
    conn.close()
    return tables, columns

# Usage
registry = load_registry("out/registry.ndjson")
tables, columns = search_tables("out/registry.db", "policy")

print("Matching tables:", tables)
print("Matching columns:", columns)

# Get full details for a table
for table_name in tables[:3]:
    if table_name in registry:
        print(f"\n{table_name}:")
        print(f"  Top columns: {registry[table_name].get('top_columns', [])}")
        print(f"  Aliases: {registry[table_name].get('aliases', [])}")
        print(f"  Sample queries: {registry[table_name].get('sample_queries', [])}")
```

## Pipeline Details

The `sql_to_registry.py` script runs a three-step pipeline:

1. **DDL to Schema** (`utils/ddl_to_schema.py`): Extracts CREATE TABLE statements and generates `schema.json`
2. **Build Registry** (`utils/build_registry.py`): Creates `registry.ndjson` and `registry.db` with table metadata and indexes
3. **Enrich Registry** (`utils/enrich_registry.py`): Adds sample queries and additional aliases to the registry

Optional validation (with `--validate` flag) tests that the generated files can be loaded by `TableRegistry`.

## Interactive Registry Enrichment

After generating the registry files, you can use the Streamlit web UI to manually add and edit sample queries, intents, and join hints for better natural language query matching.

### Running the Streamlit Enricher

First, install Streamlit if you haven't already:

```bash
pip install streamlit
```

Then run the enrichment tool:

```bash
streamlit run utils/streamlit_enrich.py
```

This will open a web interface in your browser (typically at `http://localhost:8501`).

### Features

The Streamlit enricher provides:

- **Table Selection**: Choose from existing tables or create new table entries
- **Sample Query Management**: Add natural language queries with metadata:
  - Query text (e.g., "List policies due next month for Zurich Insurance")
  - Intent tags (optional classification)
  - Confidence scores (0.0 to 1.0)
  - Join hints (table relationships and ON clauses)
- **Live Preview**: Preview the sample query object before saving
- **Schema View**: Quick reference to table columns from `schema.json`
- **Automatic Indexing**: Updates SQLite indexes with tokens from queries and joins

### Example Usage

1. **Start the app**:
   ```bash
   streamlit run utils/streamlit_enrich.py
   ```

2. **Configure output directory** (if different from default):
   - Use the sidebar to set a custom output directory
   - Default is `out/` relative to project root
   - Can also set via `REGISTRY_OUT_DIR` environment variable

3. **Add a sample query**:
   - Select a table from the dropdown (or enter a new table name)
   - Enter a natural language query: `"List policies due next month for Zurich Insurance"`
   - Optionally add an intent tag: `"policy_lookup"`
   - Set confidence: `0.9`
   - Add join hints if needed:
     ```
     Table: customer
     ON: policies.pol_customerid = customer.cust_id
     ```

4. **Preview and save**:
   - Click "Preview Sample Object" to see the JSON structure
   - Click "Save Sample Query" to update `registry.ndjson` and `registry.db`

### Sample Query Object Structure

When you save a sample query, it creates an object like this:

```json
{
  "query": "List policies due next month for Zurich Insurance",
  "intent": "policy_lookup",
  "confidence": 0.9,
  "joins": [
    {
      "table": "customer",
      "on": "policies.pol_customerid = customer.cust_id"
    }
  ]
}
```

This gets added to the `sample_queries` array in the table's registry entry, and tokens from the query and joins are automatically indexed in the SQLite database for faster lookups.

### Configuration

The tool automatically detects the project structure and uses:
- Default output directory: `out/` (relative to project root)
- Environment variable: `REGISTRY_OUT_DIR` to override default
- UI override: Sidebar input field for custom directory

All paths are resolved relative to the project root, so the tool works regardless of where you run it from.

## Project Structure

```
sql2graph/
├── sql_to_registry.py      # Main pipeline script (SQL DDL → registry files)
├── utils/
│   ├── ddl_to_schema.py    # SQL DDL parser
│   ├── build_registry.py   # Registry builder
│   ├── enrich_registry.py  # Registry enrichment
│   ├── table_selector.py   # LangChain integration classes
│   └── streamlit_enrich.py # Interactive web UI for registry enrichment
├── dbscripts/              # Sample SQL schemas
├── out/                    # Generated output files
│   ├── schema.json
│   ├── registry.ndjson
│   └── registry.db
└── README.md
```

## Quick Reference

### Generated Files Usage

| File | Purpose | Usage |
|------|---------|-------|
| `schema.json` | Full table-to-columns mapping | Get complete column lists for SQL generation |
| `registry.ndjson` | Table metadata (aliases, top columns, queries) | Load table information and metadata |
| `registry.db` | SQLite token indexes | Fast token-based table/column lookups |

### Key Classes (from `utils/table_selector.py`)

- **`TableRegistry`**: Loads and queries registry files
  - `find_tables_by_token(token)`: Find tables matching a token
  - `find_columns_by_token(token)`: Find columns matching a token
  
- **`TableSelectorTool`**: LangChain-compatible tool wrapper
  - `_run(query)`: Returns relevant tables and columns for a natural language query

- **`SchemaAwareTool`**: Tool with full schema context
  - `_run(query)`: Returns tables with complete column lists from schema.json

## Requirements

- Python 3.12+
- See `pyproject.toml` for full dependency list

## Dependencies

Key dependencies include:
- `sqlparse` - SQL parsing
- `sqlite3` - Database indexing (built-in)
- `langchain` - For agent integration (install separately: `pip install langchain openai`)
- `streamlit` - For interactive registry enrichment UI (install separately: `pip install streamlit`)

## License
MIT License
