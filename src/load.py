"""
load.py

Section 6.10 - Data Warehouse Loading and Dimensional Modeling.
Section 7.3  - Repeatability and Safe Rerun.

Writes the VALIDATED prepared data into the dimensional Data Warehouse
(music_dw). This is the final ETL Load; it is different from the earlier
Grammy source-preparation import (prepare_source.py).

Rerun strategy: ATOMIC REPLACE (truncate-and-load inside ONE transaction).
  * Every run processes a complete snapshot of both sources (not increments),
    so the DW must equal "the last successful snapshot", never a sum of runs.
  * TRUNCATE + INSERT + verification all happen in a single transaction.
    A failure at any point rolls everything back: the DW keeps the previous
    successful state. It is never left half loaded.
  * Surrogate keys are assigned in business-key order, so the same input always
    yields the same keys and the same rows (idempotent).
  * PK / UNIQUE constraints on business keys (track_id, genre_name,
    category_name, nomination_id) make duplicates impossible even if the
    truncate were bypassed.
  * etl_load_audit records every attempt (SUCCESS inside the transaction,
    FAILED after the rollback).

After a partially completed or failed run: the transaction has rolled back, the
DW still holds the previous snapshot, and the failure is visible in
etl_load_audit and in the task log. Simply rerunning is safe.
"""

import io
import json
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db import get_admin_engine, get_dw_engine

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DDL_PATH = PROJECT_ROOT / "sql" / "dw_schema.sql"

FACT_TRACK_COLUMNS = [
    "track_key", "genre_key", "popularity", "duration_ms", "danceability", "energy",
    "valence", "acousticness", "instrumentalness", "liveness", "speechiness",
    "tempo", "loudness", "musical_key", "mode", "time_signature", "is_grammy_nominated",
]
FACT_NOMINATION_COLUMNS = [
    "nomination_id", "year_key", "category_key", "track_key", "nominee", "artist",
    "winner_raw", "is_confirmed_winner", "match_type", "match_candidates",
    "match_ambiguous", "is_matched_to_spotify",
]
TABLE_ORDER = ["dim_track", "dim_genre", "dim_category", "dim_year", "fact_track", "fact_nomination"]


# ---------------------------------------------------------------------
# Dimensional model construction (pure pandas, deterministic)
# ---------------------------------------------------------------------

def build_model(tracks: pd.DataFrame, noms: pd.DataFrame) -> dict:
    # dim_track: one row per track, attributes from its highest-popularity row
    dt = (
        tracks.sort_values(["popularity", "track_id"], ascending=[False, True])
        .drop_duplicates("track_id")[["track_id", "track_name", "artists", "album_name", "explicit"]]
        .sort_values("track_id").reset_index(drop=True)
    )
    dt.insert(0, "track_key", range(1, len(dt) + 1))

    dg = pd.DataFrame({"genre_name": sorted(tracks["track_genre"].unique())})
    dg.insert(0, "genre_key", range(1, len(dg) + 1))

    dc = pd.DataFrame({"category_name": sorted(noms["category_normalized"].unique())})
    dc.insert(0, "category_key", range(1, len(dc) + 1))

    dy = (
        noms[["year", "decade"]].drop_duplicates().sort_values("year")
        .rename(columns={"year": "year_key"}).reset_index(drop=True)
    )

    track_key = dict(zip(dt["track_id"], dt["track_key"]))
    genre_key = dict(zip(dg["genre_name"], dg["genre_key"]))
    category_key = dict(zip(dc["category_name"], dc["category_key"]))

    ft = tracks.copy()
    ft["track_key"] = ft["track_id"].map(track_key)
    ft["genre_key"] = ft["track_genre"].map(genre_key)
    ft = ft.rename(columns={"key": "musical_key"})[FACT_TRACK_COLUMNS]
    ft = ft.sort_values(["track_key", "genre_key"]).reset_index(drop=True)

    fn = noms.copy()
    fn["nomination_id"] = fn["grammy_row_id"]
    fn["year_key"] = fn["year"]
    fn["category_key"] = fn["category_normalized"].map(category_key)
    fn["track_key"] = fn["matched_track_id"].map(track_key).astype("Int64")
    fn = fn[FACT_NOMINATION_COLUMNS].sort_values("nomination_id").reset_index(drop=True)

    return {
        "dim_track": dt, "dim_genre": dg, "dim_category": dc, "dim_year": dy,
        "fact_track": ft, "fact_nomination": fn,
    }


# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------

def ensure_dw_database(dw_name: str):
    """
    Create the DW database if it does not exist. The docker init script only runs
    when the PostgreSQL volume is empty, so the pipeline must not depend on it.
    """
    admin = get_admin_engine()
    try:
        with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": dw_name}
            ).scalar()
            if not exists:
                logger.warning("Database %s does not exist; creating it", dw_name)
                conn.execute(text(f'CREATE DATABASE "{dw_name}"'))
    finally:
        admin.dispose()


def create_schema(engine):
    # Strip comment lines BEFORE splitting on ';' (a ';' inside a comment would
    # otherwise cut a statement in half).
    ddl = "\n".join(
        l for l in DDL_PATH.read_text().splitlines() if not l.strip().startswith("--")
    )
    with engine.begin() as conn:
        for stmt in ddl.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.exec_driver_sql(stmt)


def _copy_df(conn, table: str, df: pd.DataFrame):
    """Bulk insert through COPY, inside the caller's transaction."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    cols = ", ".join(df.columns)
    cur = conn.connection.cursor()
    cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)


def _count(conn, table: str) -> int:
    return int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar())


# ---------------------------------------------------------------------
# Task entry point
# ---------------------------------------------------------------------

def load_dw(tracks_path: str, nominations_path: str, batch_id: str, fail_after: str = None) -> dict:
    """
    fail_after: test hook ("dims" or "fact_track") that raises mid-load to
    demonstrate rollback behaviour (Section 7.3). Never set in normal runs.
    """
    tracks = pd.read_parquet(tracks_path)
    noms = pd.read_parquet(nominations_path)
    model = build_model(tracks, noms)

    engine = get_dw_engine()
    ensure_dw_database(engine.url.database)
    create_schema(engine)

    try:
        with engine.begin() as conn:
            before = {t: _count(conn, t) for t in TABLE_ORDER}
            logger.info("[batch=%s] DW row counts BEFORE load: %s", batch_id, before)

            conn.execute(text("TRUNCATE fact_nomination, fact_track, dim_track, dim_genre, dim_category, dim_year"))

            for t in ["dim_track", "dim_genre", "dim_category", "dim_year"]:
                _copy_df(conn, t, model[t])
            if fail_after == "dims":
                raise RuntimeError("Simulated failure after loading dimensions")

            _copy_df(conn, "fact_track", model["fact_track"])
            if fail_after == "fact_track":
                raise RuntimeError("Simulated failure after loading fact_track")
            _copy_df(conn, "fact_nomination", model["fact_nomination"])

            # Verification BEFORE commit: any mismatch raises -> full rollback
            after = {t: _count(conn, t) for t in TABLE_ORDER}
            expected = {t: int(len(model[t])) for t in TABLE_ORDER}
            if after != expected:
                raise ValueError(f"Row count mismatch after load: loaded={after} expected={expected}")

            pop_sum = int(conn.execute(text("SELECT COALESCE(SUM(popularity), 0) FROM fact_track")).scalar())
            if pop_sum != int(model["fact_track"]["popularity"].sum()):
                raise ValueError("Measure checksum mismatch (SUM popularity) after load")
            matched = int(conn.execute(text(
                "SELECT COUNT(*) FROM fact_nomination WHERE track_key IS NOT NULL")).scalar())

            summary = {
                "batch_id": batch_id, "rows_before": before, "rows_after": after,
                "sum_popularity": pop_sum, "matched_nominations": matched,
            }
            conn.execute(
                text("INSERT INTO etl_load_audit (batch_id, status, summary) VALUES (:b, 'SUCCESS', CAST(:s AS JSONB))"),
                {"b": batch_id, "s": json.dumps(summary)},
            )
        logger.info("[batch=%s] DW load committed: %s", batch_id, summary)
        return summary

    except Exception as exc:
        logger.error("[batch=%s] DW load FAILED and was rolled back: %s", batch_id, exc)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("INSERT INTO etl_load_audit (batch_id, status, error) VALUES (:b, 'FAILED', :e)"),
                    {"b": batch_id, "e": str(exc)[:1000]},
                )
        except Exception:  # audit must never mask the original failure
            logger.exception("Could not write the FAILED audit row")
        raise
