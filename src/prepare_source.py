"""
prepare_source.py

Section 6.2 - Environment and Source Preparation.

Loads the provided Grammy Awards CSV into a table inside the grammy_source
PostgreSQL database. This is SOURCE PREPARATION, not the final ETL Load:
the operational pipeline (extract_grammys, in the Airflow DAG) will read
from this table, never from the CSV directly.

Preserves the original CSV untouched in data/. Documents row-count
reconciliation between the CSV and the loaded table, as required evidence.

Run manually, once, before the DAG is enabled:
    python src/prepare_source.py
"""

import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = PROJECT_ROOT / "data" / "the_grammy_awards.csv"
DDL_PATH = PROJECT_ROOT / "sql" / "source_setup.sql"


def get_source_engine():
    host = os.environ.get("MUSIC_DB_HOST", "localhost")
    port = os.environ.get("MUSIC_DB_PORT", "5433")
    user = os.environ.get("MUSIC_DB_USER", "music_user")
    password = os.environ.get("MUSIC_DB_PASSWORD", "music_pass")
    db_name = os.environ.get("MUSIC_DB_SOURCE_NAME", "grammy_source")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(url)


def create_schema(engine):
    ddl = DDL_PATH.read_text()
    with engine.begin() as conn:
        for statement in ddl.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))
    print("grammy_awards_raw table created in grammy_source.")


def load_csv(engine):
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Grammy CSV not found at: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    csv_row_count = len(df)
    print(f"CSV read OK: {csv_row_count} rows, {df.shape[1]} columns.")

    # Load exactly as provided - no cleaning here. This is a raw copy of the
    # source, not a prepared/transformed dataset.
    df.to_sql(
        "grammy_awards_raw",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
    )

    return csv_row_count


def reconcile(engine, csv_row_count):
    with engine.connect() as conn:
        db_row_count = conn.execute(
            text("SELECT COUNT(*) FROM grammy_awards_raw")
        ).scalar()

    print(f"CSV row count:    {csv_row_count}")
    print(f"Loaded row count: {db_row_count}")

    if csv_row_count != db_row_count:
        print(
            "RECONCILIATION FAILED: row counts do not match. "
            "Do not proceed until this is resolved.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Reconciliation OK: CSV and source table row counts match.")


def main():
    engine = get_source_engine()
    create_schema(engine)
    csv_row_count = load_csv(engine)
    reconcile(engine, csv_row_count)


if __name__ == "__main__":
    main()
