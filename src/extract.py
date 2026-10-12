"""
extract.py

Section 6.7 - Data Extraction.

extract_spotify  : reads the Spotify CSV file.
extract_grammys  : reads the Grammy data from the RELATIONAL SOURCE DATABASE
                   (grammy_source.grammy_awards_raw), never from the Grammy CSV.
                   This is the required evidence for Section 4.3.

Extraction does NOT clean or alter values, so source-quality problems stay
visible to the raw validation gates. Each function writes the extracted batch
to a staging parquet file and returns only compact metadata (path, row count,
batch id). Airflow tasks pass this metadata between each other; the bulk data
never travels through Airflow's metadata database (Section 6.11).
"""

import logging
from pathlib import Path

import pandas as pd

from db import get_source_engine

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SPOTIFY_CSV = PROJECT_ROOT / "data" / "spotify_dataset.csv"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

GRAMMY_SOURCE_QUERY = "SELECT * FROM grammy_awards_raw ORDER BY row_id"


def _batch_dir(batch_id: str) -> Path:
    d = STAGING_DIR / batch_id
    d.mkdir(parents=True, exist_ok=True)
    return d


SPOTIFY_FAULTS = ("none", "spotify_duration_ms_zero")


def extract_spotify(batch_id: str, fault: str = "none") -> dict:
    """
    fault: TEST HOOK for Section 7.2 (controlled Critical failure). It simulates a
    corrupted source batch: extraction still succeeds, and the raw validation
    gate must then catch the violation. Only reachable through the DAG parameter
    `inject_spotify_fault`; the default "none" leaves the data untouched.
    """
    if fault not in SPOTIFY_FAULTS:
        raise ValueError(f"Unknown fault '{fault}'. Allowed: {SPOTIFY_FAULTS}")
    if not SPOTIFY_CSV.exists():
        raise FileNotFoundError(f"Spotify CSV not found at {SPOTIFY_CSV}")

    logger.info("[batch=%s] Extracting Spotify from CSV file: %s", batch_id, SPOTIFY_CSV)
    df = pd.read_csv(SPOTIFY_CSV)

    if fault == "spotify_duration_ms_zero":
        idx = df.sample(frac=0.05, random_state=1).index
        df.loc[idx, "duration_ms"] = 0
        logger.warning(
            "[batch=%s] FAULT INJECTION ACTIVE (test hook): duration_ms set to 0 on %d rows (5%%)",
            batch_id, len(idx),
        )

    out_path = _batch_dir(batch_id) / "spotify_raw.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("[batch=%s] Spotify extracted: %d rows -> %s", batch_id, len(df), out_path)

    return {
        "batch_id": batch_id,
        "source": "spotify_csv",
        "path": str(out_path),
        "row_count": int(len(df)),
        "fault_injected": fault,
    }


def extract_grammys(batch_id: str) -> dict:
    logger.info(
        "[batch=%s] Extracting Grammy from relational source database "
        "(grammy_source), query: %s", batch_id, GRAMMY_SOURCE_QUERY,
    )
    engine = get_source_engine()
    try:
        df = pd.read_sql(GRAMMY_SOURCE_QUERY, engine)
    except pd.errors.DatabaseError as exc:
        # pandas wraps the SQLAlchemy error in its own exception. Re-raise the original
        # so the DAG can tell deterministic errors (table missing) from transient ones.
        raise (exc.__cause__ or exc)

    out_path = _batch_dir(batch_id) / "grammy_raw.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("[batch=%s] Grammy extracted: %d rows -> %s", batch_id, len(df), out_path)

    return {
        "batch_id": batch_id,
        "source": "grammy_source.grammy_awards_raw (PostgreSQL)",
        "path": str(out_path),
        "row_count": int(len(df)),
    }
