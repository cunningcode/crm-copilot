import os, pandas as pd, streamlit as st
from db import get_db_engine, reflect_schema, schema_to_prompt, is_query_safe, ensure_limit, run_sql, DuckDemo
from sql_agent import generate_sql, summarize_answer

st.set_page_config(page_title="PMC CRM Copilot", page_icon="🧠", layout="wide")
st.title("🧠 PMC CRM Copilot — Ask your CRM in plain English")

with st.sidebar:
    st.header("Settings")
    OPENAI_OK = bool(os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY"))
    st.write("🔑 OpenAI:", "✅ found" if OPENAI_OK else "❌ missing")

    DIALECT = os.getenv("DIALECT") or st.secrets.get("DIALECT", "postgresql")
    ROW_LIMIT = int(os.getenv("ROW_LIMIT") or st.secrets.get("ROW_LIMIT", "1000"))
    allow_tables = st.secrets.get("ALLOW_TABLES", "")
    allow_tables = [t.strip() for t in allow_tables.split(",") if t.strip()] if isinstance(allow_tables, str) else allow_tables
    pii_blocklist = st.secrets.get("PII_BLOCKLIST", "email,phone,address")
    pii_blocklist = [c.strip().lower() for c in pii_blocklist.split(",")]

    st.caption(f"Dialect: {DIALECT} · Row cap: {ROW_LIMIT}")

    MODE = st.radio("Data source", ["Database", "Demo (CSV → DuckDB)"], index=0)

    db_url = os.getenv("DB_URL") or st.secrets.get("DB_URL", "")
    if MODE == "Database":
        if db_url: st.success("DB URL is set.")
        else: st.error("DB_URL is not set in Secrets. Switch to Demo mode or add a connection string.")
    else:
        st.info("Demo mode: upload CSVs to create temporary tables.")

st.divider()

# Build schema text the model will see
if MODE == "Database" and db_url:
    engine = get_db_engine(db_url)
    with st.spinner("Reflecting database schema…"):
        schema = reflect_schema(engine, include_tables=allow_tables or None)
    schema_text = schema_to_prompt(schema, dialect=DIALECT, pii_blocklist=pii_blocklist)
else:
    engine = None
    demo = DuckDemo()
    st.subheader("📥 Demo mode: Upload CSV to create tables")
    uploaded = st.file_uploader("Upload one or more CSV files", type=["csv"], accept_multiple_files=True)
    if uploaded:
        for f in uploaded:
            name = os.path.splitext(f.name)[0].replace("-", "_").replace(" ", "_")
            df = pd.read_csv(f)
            demo.register_df(name, df)
        st.success("Loaded tables: " + ", ".join(demo.list_tables()))
    schema = demo.reflect()
    schema_text = schema_to_prompt(schema, dialect="duckdb", pii_blocklist=pii_blocklist)

with st.expander("🔎 Schema (what the model sees)"):
    st.code(schema_text, language="markdown")

st.subheader("Ask a question")
question = st.text_input(
    "Try: “How many riders raised above $10k last year?” or “Top 10 teams by total raised in 2024”",
    value="How many riders raised above $10k last year?"
)
go = st.button("Ask")

if go and question.strip():
    with st.spinner("Drafting SQL…"):
        sql = generate_sql(question, schema_text=schema_text, dialect=DIALECT, pii_blocklist=pii_blocklist)
    safe, reason = is_query_safe(sql)
    if not safe:
        st.error(f"Unsafe SQL rejected: {reason}")
        st.code(sql, language="sql")
        st.stop()

    sql_limited = ensure_limit(sql, max_rows=ROW_LIMIT)
    st.markdown("**Proposed SQL**")
    st.code(sql_limited, language="sql")

    if st.button("Run query"):
        try:
            with st.spinner("Running query…"):
                df = run_sql(engine, sql_limited) if engine is not None else demo.sql(sql_limited)
        except Exception as e:
            st.warning(f"Initial query failed: {e}")
            with st.spinner("Trying an automatic fix…"):
                amended = question + f"\n(Note: previous SQL error was: {e}. Please correct it.)"
                sql2 = generate_sql(amended, schema_text=schema_text, dialect=DIALECT, pii_blocklist=pii_blocklist)
                safe2, reason2 = is_query_safe(sql2)
                if not safe2:
                    st.error(f"Second attempt also unsafe: {reason2}")
                    st.code(sql2, language="sql"); st.stop()
                sql2 = ensure_limit(sql2, max_rows=ROW_LIMIT)
                st.markdown("**Amended SQL**")
                st.code(sql2, language="sql")
                df = run_sql(engine, sql2) if engine is not None else demo.sql(sql2)

        st.success(f"Returned {len(df):,} row(s).")
        st.dataframe(df, use_container_width=True)

        if len(df) > 0:
            with st.spinner("Summarizing result…"):
                summary = summarize_answer(question, df)
            st.markdown("### Answer")
            st.write(summary)

st.caption("Guardrails: read-only; LIMIT enforced; PII columns discouraged. Configure allowlist and blocklist via Secrets.")
