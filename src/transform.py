"""
transform.py

Section 6.8 - Data Transformation and Integration (single engineering phase).
Full decision record and integration contract: docs/transformation_integration.md

Inputs  : staging parquet files of the batch (raw, as extracted).
Outputs : staging parquet files with the PREPARED data model inputs
            tracks_prepared.parquet        grain (track_id, track_genre)  -> FactTrack
            nominations_prepared.parquet   grain one Grammy nomination    -> FactNomination
            quarantine_spotify.parquet     rows that cannot be used safely
          plus a reconciliation JSON in results/reconciliation/.
Returns compact metadata only (paths + counts); bulk data never travels
through Airflow's metadata store.

Transformation rules (rule / affected fields / exception handling):
  T1  Drop the `Unnamed: 0` export-artifact column (Spotify).
  T2  Quarantine Spotify rows with duration_ms <= 0 or null track_name/artists
      (DQ1/DQ3). Quarantined rows are kept with a reason, never silently lost.
  T3  Deduplicate (track_id, track_genre) keeping the highest popularity (DQ5).
  T4  Normalize Grammy category labels (case/spacing) to one canonical label
      per category (most frequent spelling) -> category_normalized (DQ6).
  T5  Derive is_confirmed_winner: `winner` is only trustworthy when the
      (year, category) group has exactly one row (DQ8).
  T6  Integration with Spotify, two tiers, recorded in match_type:
        exact       normalized title + full artist string equal
        normalized  title cleaned of (...)/[...]/" - suffix" + PRIMARY artist
      Null keys never match. When several Spotify tracks share a key, the one
      with the highest popularity is chosen (ties -> lowest track_id) and the
      row is flagged match_ambiguous. Result: at most ONE track per nomination,
      so the join cannot multiply Grammy rows.

Reconciliation invariants (violations raise -> deterministic failure):
  Spotify : rows_in == rows_prepared + rows_quarantined + duplicates_removed
  Grammy  : rows_in == rows_prepared, grammy_row_id unique
"""

import json
import logging
import re
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = PROJECT_ROOT / "data" / "staging"
RECON_DIR = PROJECT_ROOT / "results" / "reconciliation"


# ---------------------------------------------------------------------
# Text normalization helpers (used only to build matching keys)
# ---------------------------------------------------------------------

def _base(s):
    if not isinstance(s, str):
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return s.lower().strip()


def _punct(s):
    if not isinstance(s, str):
        return None
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _clean_title(s):
    if not isinstance(s, str):
        return None
    s = re.sub(r"\(.*?\)|\[.*?\]", " ", s)
    s = re.split(r"\s-\s", s)[0]
    return _punct(s) or None


def _primary_artist_spotify(s):
    if not isinstance(s, str):
        return None
    return _punct(s.split(";")[0]) or None


def _primary_artist_grammy(s):
    if not isinstance(s, str):
        return None
    s = re.split(r"\s+(?:featuring|feat\.?|ft\.?|&|and|with|x)\s+|,|/|;", s)[0]
    return _punct(s) or None


# ---------------------------------------------------------------------
# Spotify
# ---------------------------------------------------------------------

def transform_spotify(df: pd.DataFrame):
    stats = {"rows_in": int(len(df))}

    df = df.drop(columns=["Unnamed: 0"], errors="ignore")                       # T1

    reason = pd.Series("", index=df.index)                                       # T2
    reason[df["duration_ms"] <= 0] += "duration_ms<=0;"
    reason[df["track_name"].isna()] += "track_name_null;"
    reason[df["artists"].isna()] += "artists_null;"
    bad = reason != ""
    quarantine = df[bad].assign(quarantine_reason=reason[bad])
    clean = df[~bad]
    stats["rows_quarantined"] = int(len(quarantine))

    clean = clean.sort_values(["popularity", "track_id"], ascending=[False, True])  # T3
    before = len(clean)
    clean = clean.drop_duplicates(["track_id", "track_genre"], keep="first")
    stats["duplicates_removed"] = int(before - len(clean))

    # Track-level candidate table for integration (one row per track_id,
    # highest-popularity row first thanks to the sort above).
    cand = clean.drop_duplicates("track_id")[["track_id", "track_name", "artists", "popularity"]].copy()
    cand["t_exact"] = cand["track_name"].map(_base)
    cand["a_exact"] = cand["artists"].map(_base)
    cand["t_clean"] = cand["t_exact"].map(_clean_title)
    cand["a_prim"] = cand["a_exact"].map(_primary_artist_spotify)

    tracks = clean.sort_values(["track_id", "track_genre"]).reset_index(drop=True)
    stats["rows_prepared"] = int(len(tracks))

    if stats["rows_in"] != stats["rows_prepared"] + stats["rows_quarantined"] + stats["duplicates_removed"]:
        raise ValueError(f"Spotify reconciliation failed: {stats}")
    stats["distinct_tracks"] = int(tracks["track_id"].nunique())
    return tracks, quarantine, cand, stats


# ---------------------------------------------------------------------
# Grammy
# ---------------------------------------------------------------------

def _lookup(cand, tcol, acol):
    c = cand.dropna(subset=[tcol, acol]).sort_values(["popularity", "track_id"], ascending=[False, True])
    return (
        c.groupby([tcol, acol])
        .agg(best_track_id=("track_id", "first"), n_candidates=("track_id", "nunique"))
        .reset_index()
    )


def _match_tier(g, idx, gt, ga, lookup, tcol, acol):
    sub = g.loc[idx, [gt, ga]].dropna()
    if sub.empty:
        return pd.DataFrame(columns=["best_track_id", "n_candidates"])
    sub = sub.reset_index(names="_ix").merge(lookup, left_on=[gt, ga], right_on=[tcol, acol], how="inner")
    return sub.set_index("_ix")[["best_track_id", "n_candidates"]]


def transform_and_integrate_grammy(gr: pd.DataFrame, cand: pd.DataFrame):
    stats = {"rows_in": int(len(gr))}
    g = gr.copy()

    g["grammy_row_id"] = g["row_id"]
    g["category_raw"] = g["category"]

    # T4 - canonical category label
    g["_ckey"] = g["category"].str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
    counts = g.groupby(["_ckey", "category_raw"]).size().reset_index(name="n")
    canon = (
        counts.sort_values(["_ckey", "n", "category_raw"], ascending=[True, False, True])
        .drop_duplicates("_ckey").set_index("_ckey")["category_raw"]
    )
    g["category_normalized"] = g["_ckey"].map(canon)
    stats["categories_raw"] = int(g["category_raw"].nunique())
    stats["categories_normalized"] = int(g["category_normalized"].nunique())

    # T5 - winner reliability
    g["winner_raw"] = g["winner"]
    gsize = g.groupby(["year", "_ckey"])["year"].transform("size")
    g["is_confirmed_winner"] = (gsize == 1) & g["winner_raw"].astype(bool)
    g["decade"] = (g["year"] // 10) * 10

    # T6 - integration
    g["t_exact"] = g["nominee"].map(_base)
    g["a_exact"] = g["artist"].map(_base)
    g["t_clean"] = g["t_exact"].map(_clean_title)
    g["a_prim"] = g["a_exact"].map(_primary_artist_grammy)

    g["matched_track_id"] = pd.NA
    g["match_type"] = "none"
    g["match_candidates"] = 0

    unmatched = g.index
    m1 = _match_tier(g, unmatched, "t_exact", "a_exact", _lookup(cand, "t_exact", "a_exact"), "t_exact", "a_exact")
    g.loc[m1.index, "matched_track_id"] = m1["best_track_id"]
    g.loc[m1.index, "match_type"] = "exact"
    g.loc[m1.index, "match_candidates"] = m1["n_candidates"]

    unmatched = g.index[g["match_type"] == "none"]
    m2 = _match_tier(g, unmatched, "t_clean", "a_prim", _lookup(cand, "t_clean", "a_prim"), "t_clean", "a_prim")
    g.loc[m2.index, "matched_track_id"] = m2["best_track_id"]
    g.loc[m2.index, "match_type"] = "normalized"
    g.loc[m2.index, "match_candidates"] = m2["n_candidates"]

    g["match_candidates"] = g["match_candidates"].astype(int)
    g["match_ambiguous"] = g["match_candidates"] > 1
    g["is_matched_to_spotify"] = g["match_type"] != "none"
    g["is_matched_to_spotify_int"] = g["is_matched_to_spotify"].astype(int)

    out = g[[
        "grammy_row_id", "year", "decade", "category_raw", "category_normalized",
        "nominee", "artist", "winner_raw", "is_confirmed_winner",
        "matched_track_id", "match_type", "match_candidates", "match_ambiguous",
        "is_matched_to_spotify", "is_matched_to_spotify_int",
    ]].reset_index(drop=True)

    stats["rows_prepared"] = int(len(out))
    if stats["rows_in"] != stats["rows_prepared"] or not out["grammy_row_id"].is_unique:
        raise ValueError(f"Grammy reconciliation failed (row loss or fan-out): {stats}")

    vc = out["match_type"].value_counts()
    stats.update({
        "matched_exact": int(vc.get("exact", 0)),
        "matched_normalized": int(vc.get("normalized", 0)),
        "unmatched": int(vc.get("none", 0)),
        "match_rate": round(float(out["is_matched_to_spotify"].mean()), 4),
        "ambiguous_matches": int(out["match_ambiguous"].sum()),
        "confirmed_winners": int(out["is_confirmed_winner"].sum()),
        "null_key_rows_unmatched": int((out["nominee"].isna() | out["artist"].isna()).sum()),
    })
    return out, stats


# ---------------------------------------------------------------------
# Task entry point
# ---------------------------------------------------------------------

def transform_and_integrate(spotify_path: str, grammys_path: str, batch_id: str) -> dict:
    logger.info("[batch=%s] transform_and_integrate: reading staged raw data", batch_id)
    sp = pd.read_parquet(spotify_path)
    gr = pd.read_parquet(grammys_path)

    tracks, quarantine, cand, sp_stats = transform_spotify(sp)
    noms, gr_stats = transform_and_integrate_grammy(gr, cand)

    # Derived flag on the Spotify side (R1): track has at least one Grammy nomination
    nominated_ids = set(noms["matched_track_id"].dropna())
    tracks["is_grammy_nominated"] = tracks["track_id"].isin(nominated_ids)
    sp_stats["tracks_nominated"] = int(tracks.loc[tracks["is_grammy_nominated"], "track_id"].nunique())

    out_dir = STAGING_DIR / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "tracks_path": str(out_dir / "tracks_prepared.parquet"),
        "nominations_path": str(out_dir / "nominations_prepared.parquet"),
        "quarantine_path": str(out_dir / "quarantine_spotify.parquet"),
    }
    tracks.to_parquet(paths["tracks_path"], index=False)
    noms.to_parquet(paths["nominations_path"], index=False)
    quarantine.to_parquet(paths["quarantine_path"], index=False)

    recon = {
        "batch_id": batch_id,
        "executed_at": datetime.utcnow().isoformat() + "Z",
        "spotify": sp_stats,
        "grammy": gr_stats,
    }
    RECON_DIR.mkdir(parents=True, exist_ok=True)
    (RECON_DIR / f"{batch_id}__reconciliation.json").write_text(json.dumps(recon, indent=2))

    logger.info("[batch=%s] Spotify: %s", batch_id, sp_stats)
    logger.info("[batch=%s] Grammy: %s", batch_id, gr_stats)
    return {"batch_id": batch_id, **paths, "spotify": sp_stats, "grammy": gr_stats}


def inject_join_fanout(nominations_path: str, n: int = 190) -> int:
    """
    TEST HOOK for the prepared-gate failure test (Section 6.9 / 7.2). It simulates an
    integration bug (a join that multiplies rows) by appending `n` duplicated nominations
    to the prepared file. The transformation's own reconciliation has already passed, so
    the prepared validation gate (DQ10) is the one that must catch it and block load_dw.
    Only reachable through the DAG parameter `inject_prepared_fault`.
    """
    df = pd.read_parquet(nominations_path)
    extra = df.sample(n=n, random_state=1)
    pd.concat([df, extra], ignore_index=True).to_parquet(nominations_path, index=False)
    return n
