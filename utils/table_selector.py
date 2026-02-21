"""
Table Registry and Selector Tool for LangChain Integration

This module provides classes to load and query the generated registry files
(schema.json, registry.ndjson, registry.db) for use with LangChain agents.
"""
import json
import sqlite3
from typing import List, Dict, Tuple, Optional


class TableRegistry:
    """Loads and queries the generated registry files."""
    
    def __init__(self, ndjson_path: str, db_path: str, schema_path: Optional[str] = None):
        """
        Initialize the registry loader.
        
        Args:
            ndjson_path: Path to registry.ndjson file
            db_path: Path to registry.db SQLite file
            schema_path: Optional path to schema.json file
        """
        self.ndjson_path = ndjson_path
        self.db_path = db_path
        self.schema = {}
        if schema_path:
            with open(schema_path, 'r', encoding='utf8') as f:
                self.schema = json.load(f)
        self._load_registry()
    
    def _load_registry(self):
        """Load registry from NDJSON file."""
        self.tables = {}
        with open(self.ndjson_path, 'r', encoding='utf8') as f:
            for line in f:
                doc = json.loads(line)
                self.tables[doc['table']] = doc
    
    def find_tables_by_token(self, token: str, limit: int = 5) -> List[Dict]:
        """
        Find tables matching a token using SQLite index.
        
        Args:
            token: Search token (will be normalized)
            limit: Maximum number of results
            
        Returns:
            List of table document dictionaries
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        normalized = token.lower().replace(' ', '_')
        cur.execute(
            "SELECT DISTINCT table_name FROM alias_index WHERE token = ? LIMIT ?",
            (normalized, limit)
        )
        results = cur.fetchall()
        conn.close()
        table_names = [r[0] for r in results]
        return [self.tables.get(t) for t in table_names if t in self.tables]
    
    def find_columns_by_token(self, token: str, limit: int = 10) -> List[Tuple[str, str]]:
        """
        Find (table, column) pairs matching a token.
        
        Args:
            token: Search token (will be normalized)
            limit: Maximum number of results
            
        Returns:
            List of (table_name, column_name) tuples
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        normalized = token.lower().replace(' ', '_')
        cur.execute(
            "SELECT DISTINCT table_name, column_name FROM column_index WHERE token = ? LIMIT ?",
            (normalized, limit)
        )
        results = cur.fetchall()
        conn.close()
        return results
    
    def close(self):
        """Close any open connections."""
        pass


class TableSelectorTool:
    """LangChain-compatible tool for table selection."""
    
    def __init__(self, registry: TableRegistry):
        """
        Initialize the table selector tool.
        
        Args:
            registry: TableRegistry instance
        """
        self.registry = registry
    
    def _run(self, query: str) -> str:
        """
        Find relevant tables and columns for a natural language query.
        
        Args:
            query: Natural language query string
            
        Returns:
            JSON string with relevant tables and columns
        """
        # Simple token extraction (in production, use NLP)
        tokens = query.lower().split()
        results = []
        
        for token in tokens:
            tables = self.registry.find_tables_by_token(token, limit=3)
            columns = self.registry.find_columns_by_token(token, limit=5)
            
            for table in tables:
                if table:
                    results.append({
                        'table': table['table'],
                        'top_columns': table.get('top_columns', []),
                        'aliases': table.get('aliases', [])
                    })
        
        # Deduplicate
        seen = set()
        unique_results = []
        for r in results:
            if r['table'] not in seen:
                seen.add(r['table'])
                unique_results.append(r)
        
        return json.dumps(unique_results[:5], indent=2)


class SchemaAwareTool:
    """Tool that uses full schema information for better context."""
    
    def __init__(self, registry: TableRegistry):
        """
        Initialize the schema-aware tool.
        
        Args:
            registry: TableRegistry instance
        """
        self.registry = registry
    
    def _run(self, query: str) -> str:
        """
        Return tables with full column lists from schema.
        
        Args:
            query: Natural language query string
            
        Returns:
            JSON string with tables and full column information
        """
        tokens = query.lower().split()
        results = []
        
        for token in tokens:
            tables = self.registry.find_tables_by_token(token, limit=3)
            for table_doc in tables:
                if table_doc:
                    table_name = table_doc['table']
                    # Get full column list from schema
                    all_columns = self.registry.schema.get(table_name, [])
                    results.append({
                        'table': table_name,
                        'top_columns': table_doc.get('top_columns', []),
                        'all_columns': all_columns,
                        'sample_queries': table_doc.get('sample_queries', [])
                    })
        
        # Deduplicate
        seen = set()
        unique_results = []
        for r in results:
            if r['table'] not in seen:
                seen.add(r['table'])
                unique_results.append(r)
        
        return json.dumps(unique_results[:3], indent=2)
