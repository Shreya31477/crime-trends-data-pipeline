"""L — Load: upsert the transformed data into PostgreSQL.

Unlike `df.to_sql(if_exists='replace')`, this keeps the table (and its PRIMARY KEY)
and inserts new incidents / updates changed ones, so daily runs are safe to repeat.
"""
import pandas as pd

from config import TABLE, get_connection
from prefect_compat import task

COLUMNS = ["id", "date_time", "type", "subtype", "location", "description", "last_updated",
           "year", "month", "day", "hour", "minute", "second", "day_of_week"]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id           BIGINT PRIMARY KEY,
    date_time    TIMESTAMP NOT NULL,
    type         TEXT NOT NULL,
    subtype      TEXT,
    location     TEXT NOT NULL,
    description  TEXT,
    last_updated TIMESTAMP,
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    hour         INTEGER,
    minute       INTEGER,
    second       INTEGER,
    day_of_week  TEXT
);
CREATE INDEX IF NOT EXISTS idx_{TABLE}_date_time ON {TABLE} (date_time);
CREATE INDEX IF NOT EXISTS idx_{TABLE}_type      ON {TABLE} (type);
"""

_updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "id")
UPSERT_SQL = f"""
INSERT INTO {TABLE} ({", ".join(COLUMNS)}) VALUES %s
ON CONFLICT (id) DO UPDATE SET {_updates};
"""


def create_postgres_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def _to_records(df: pd.DataFrame):
    """DataFrame -> list of tuples with NaN/NaT converted to None and numpy types to Python."""
    out = df[COLUMNS].astype(object).where(df[COLUMNS].notna(), None)
    records = []
    for row in out.itertuples(index=False):
        records.append(tuple(v.to_pydatetime() if isinstance(v, pd.Timestamp)
                             else (v.item() if hasattr(v, "item") else v) for v in row))
    return records


@task
def load_into_postgres(df: pd.DataFrame) -> int:
    from psycopg2.extras import execute_values

    conn = get_connection()
    try:
        create_postgres_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            before = cur.fetchone()[0]
            execute_values(cur, UPSERT_SQL, _to_records(df), page_size=5000)
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            after = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    print(f"Loaded {len(df):,} rows - {after - before:,} new incidents, {after:,} total in {TABLE}")
    return after
