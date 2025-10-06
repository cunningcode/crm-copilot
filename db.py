import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
import duckdb

def get_db_engine(db_url: str | None) -> Engine | None:
    if not db_url:
        return None
    # Read-only is enforced in app logic; ensure DB user is read-only too.
    return sa.create_engine(db_url, pool_pre_ping=True)

def reflect_schema(engine: Engine, include_tables: list[str] | None = None,
                   exclude_tables: list[str] | None = None, max_cols_per_table: int = 40) -> dict:
    insp = inspect(engine)
    schema = {}
    tables = insp.get_table_names()
    if include_tables:
        tables = [t for t in tables if t in include_tables]
    if exclude_tables:
        tables = [t for t in tables if t not in exclude_tables]
    for t in tables:
        try:
            cols = insp.get_columns(t)
            schema[t] = [f"{c['name']} {str(c.get('type') or '')}" for c in cols][:max_cols_per_table]
        except Exception:
            continue
    return schema

def schema_to_prompt(schema: dict, dialect: str = "postgresql", pii_blocklist: list[str] | None = None) -> str:
    lines = [f"SQL dialect: {dialect}", "Available tables and columns:"]
    for t, cols in schema.items():
        lines.append(f"- {t}({', '.join(cols)})")
    if pii_blocklist:
        lines.append(f"Columns containing sensitive PII that must not be selected or shown: {', '.join(pii_blocklist)}")
    return "\n".join(lines)

def is_query_safe(sql: str) -> tuple[bool, str]:
    s = sql.strip().lower()
    forbidden = ["insert ", "update ", "delete ", "drop ", "alter ", "truncate ", "create ", "grant ", "revoke "]
    if any(tok in s for tok in forbidden): return False, "Only read-only SELECT queries are allowed."
    if not s.startswith("select"): return False, "Query must start with SELECT."
    return True, ""

def ensure_limit(sql: str, max_rows: int = 1000) -> str:
    s = sql.strip().rstrip(";")
    if " limit " in s.lower(): return s + ";"
    return s + f" LIMIT {max_rows};"

def run_sql(engine: Engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)

# -------- Demo mode using DuckDB (upload CSVs as temp tables) --------
class DuckDemo:
    def __init__(self):
        self.con = duckdb.connect(database=":memory:")
    def register_df(self, name: str, df: pd.DataFrame):
        self.con.register(name, df)
    def list_tables(self) -> list[str]:
        return [r[0] for r in self.con.execute("show tables").fetchall()]
    def reflect(self) -> dict:
        out = {}
        for t in self.list_tables():
            cols = self.con.execute(f"PRAGMA table_info('{t}')").fetchall()
            out[t] = [f"{c[1]} {c[2]}" for c in cols]  # name, type
        return out
    def sql(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).fetchdf()
