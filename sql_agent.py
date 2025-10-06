import os, re, sqlparse, pandas as pd
from typing import List
from langchain_openai import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage

SYSTEM = """You are a helpful analytics copilot for the Pan-Mass Challenge (PMC) rider CRM.
Your job is to produce a SINGLE SQL query that answers the user's question.

Rules:
- SQL DIALECT: {dialect}
- Use ONLY the tables/columns listed below.
- NEVER access information_schema or system tables.
- READ-ONLY: Only SELECT queries; never modify data.
- Respect privacy: Do not select direct PII columns ({pii_blocklist}).
- If the user forgets a time frame, assume the latest full ride year in the data.
- Keep queries efficient and add reasonable filters.
- Return ONLY a code block with SQL (```sql ... ```), nothing else.
- If a count is requested, aggregate in SQL using COUNT/SUM and return minimal data.
- If top-N is requested, add ORDER BY and LIMIT.
"""

USER_PREFIX = """The database schema is:
{schema}

User question: {question}
"""

def extract_sql_from_text(text: str) -> str:
    # Prefer fenced block
    m = re.search(r"```sql(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if m: return m.group(1).strip()
    # Fallback: first statement
    parsed = sqlparse.parse(text)
    if parsed: return str(parsed[0]).strip()
    return text.strip()

def generate_sql(question: str, schema_text: str, dialect: str = "postgresql",
                 pii_blocklist: list[str] | None = None, model: str = None, temperature: float = 0.0) -> str:
    llm = ChatOpenAI(model=model or os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=temperature)
    sys = SYSTEM.format(dialect=dialect, pii_blocklist=", ".join(pii_blocklist or []))
    user = USER_PREFIX.format(schema=schema_text, question=question)
    resp = llm.invoke([SystemMessage(content=sys), HumanMessage(content=user)])
    return extract_sql_from_text(resp.content)

def summarize_answer(question: str, df: pd.DataFrame, model: str = None) -> str:
    head = df.head(50).to_csv(index=False)
    llm = ChatOpenAI(model=model or os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=0.2)
    prompt = f"""You are helping summarize SQL results for a non-technical user.
Question: {question}

Here are the first rows of the result CSV (may be truncated):
{head}

Write a concise, friendly answer in 1–3 sentences. Include the key number(s) and year if present."""
    return llm.invoke(prompt).content.strip()
