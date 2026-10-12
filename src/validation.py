"""
validation.py

Section 6.6 - Great Expectations Design.

Builds a Great Expectations File Data Context under gx/, with three
Expectation Suites that implement the quality rules DQ1-DQ9
(see docs/quality_rules.md):

    spotify_raw_suite   -> gate validate_spotify_raw
    grammy_raw_suite    -> gate validate_grammys_raw
    prepared_suite      -> gate validate_prepared

GX design elements (Section 6.6 table):
    Expectation          one GX check per declared rule (meta carries rule_id + severity)
    Expectation Suite    one per dataset/layer (above)
    Validation Definition links the suite to a whole-DataFrame batch definition
    Checkpoint           runs the validation definition; run_*_validation() wraps it

IMPORTANT - severity is evaluated per rule, not through GX's global `success`.
GX reports success=False if ANY expectation fails, including Warning-level
ones. The DAG policy is:
    Critical failure      -> stop the affected downstream path
    Warning failure       -> record, expose, continue
    Informational failure -> record only
so each run_*_validation() returns a summary with `critical_passed`, which is
what the DAG gates on. The full per-rule evidence is also persisted as JSON in
results/validation/ (machine-readable evidence for successful AND failed runs).

Threshold rationale (Section 6.5 KEY IDEA - thresholds are engineering
decisions, not "whatever lets the batch pass"):

  DQ1 / DQ3  tolerance 0.1% (mostly=0.999). Profiling found 1 row in 114,000
             (0.0009%) with duration_ms == 0 and 1 row with null track_name /
             artists / album_name. An isolated defect of that size does not
             affect aggregates (R1-R3 average over >100k rows) and is handled
             by quarantining the row in transform_and_integrate. 0.1% is about
             100x the observed defect rate: tolerant of isolated glitches,
             but a systemic problem (e.g. a broken export) trips it and stops
             the pipeline. A zero-tolerance rule would block every run on the
             provided batch for a one-row defect.
  DQ2 / DQ4 / DQ7  zero tolerance. Popularity, audio features and award year
             have hard definitional bounds; any violation indicates a unit or
             schema change in the source, not an isolated glitch.
  DQ8        mostly=0.70. Observed reliable-winner share is ~90%; a fall below
             70% would mean the nominee-list format (unreliable flag) is
             spreading and winner-based analysis can no longer be trusted.
  DQ9        band 4%-15%. Observed match rate is 5.5% (counted per distinct
             Grammy nomination). 4% is the exact-match-only level (3.8%): below
             it the normalized tier has stopped contributing. 15% is ~3x the
             observed rate: above it, over-matching is likely. Informational:
             it flags integration regressions without blocking.
"""

import json
from datetime import datetime
from pathlib import Path

import great_expectations as gx
from great_expectations.data_context import AbstractDataContext

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GX_PROJECT_ROOT = PROJECT_ROOT / "gx"
EVIDENCE_DIR = PROJECT_ROOT / "results" / "validation"

AUDIO_FEATURES_0_1 = [
    "danceability", "energy", "valence",
    "acousticness", "instrumentalness", "liveness", "speechiness",
]

CURRENT_YEAR = 2026  # upper bound for DQ7


# ---------------------------------------------------------------------
# GX context plumbing (all objects rebuilt on every call -> idempotent)
# ---------------------------------------------------------------------

def get_context() -> AbstractDataContext:
    GX_PROJECT_ROOT.mkdir(parents=True, exist_ok=True)
    return gx.get_context(mode="file", project_root_dir=str(GX_PROJECT_ROOT))


def _get_or_add_pandas_datasource(context):
    try:
        return context.data_sources.get("pandas_ds")
    except Exception:
        return context.data_sources.add_pandas(name="pandas_ds")


def _get_or_add_asset(datasource, name):
    try:
        return datasource.get_asset(name)
    except Exception:
        return datasource.add_dataframe_asset(name=name)


def _get_or_add_batch_definition(asset, name):
    try:
        return asset.get_batch_definition(name)
    except Exception:
        return asset.add_batch_definition_whole_dataframe(name)


def _rebuild_suite(context, name, expectations):
    try:
        context.suites.delete(name)
    except Exception:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=name))
    for exp in expectations:
        suite.add_expectation(exp)
    suite.save()
    return suite


def _rebuild_validation_definition(context, name, batch_def, suite):
    try:
        context.validation_definitions.delete(name)
    except Exception:
        pass
    return context.validation_definitions.add(
        gx.ValidationDefinition(name=name, data=batch_def, suite=suite)
    )


def _rebuild_checkpoint(context, name, validation_def):
    try:
        context.checkpoints.delete(name)
    except Exception:
        pass
    return context.checkpoints.add(
        gx.Checkpoint(name=name, validation_definitions=[validation_def])
    )


def _build_checkpoint(context, prefix, asset_name, expectations):
    ds = _get_or_add_pandas_datasource(context)
    asset = _get_or_add_asset(ds, asset_name)
    batch_def = _get_or_add_batch_definition(asset, f"{prefix}_batch")
    suite = _rebuild_suite(context, f"{prefix}_suite", expectations)
    vdef = _rebuild_validation_definition(context, f"{prefix}_validation", batch_def, suite)
    return _rebuild_checkpoint(context, f"{prefix}_checkpoint", vdef)


# ---------------------------------------------------------------------
# Result summarisation - per-rule severity policy + persisted evidence
# ---------------------------------------------------------------------

SEVERITY_ORDER = ["Critical", "Warning", "Informational"]


def summarize_results(results, suite_name: str, batch_id: str) -> dict:
    per_expectation = []
    results_list = results if isinstance(results, (list, tuple)) else [results]
    all_run_results = [vr for r in results_list for vr in r.run_results.values()]
    for vr in all_run_results:
        for res in vr["results"]:
            cfg = res["expectation_config"]
            meta = cfg.get("meta", {}) or {}
            kwargs = cfg.get("kwargs", {}) or {}
            r = res.get("result", {}) or {}
            per_expectation.append({
                "rule_id": meta.get("rule_id"),
                "severity": meta.get("severity"),
                "expectation_type": cfg.get("type"),
                "target": kwargs.get("column") or kwargs.get("column_list"),
                "success": bool(res["success"]),
                "unexpected_count": r.get("unexpected_count"),
                "unexpected_percent": r.get("unexpected_percent"),
                "element_count": r.get("element_count"),
            })

    # A rule fails if ANY of its expectations fails (e.g. DQ4 spans 7 columns).
    rules = {}
    for e in per_expectation:
        rid = e["rule_id"]
        entry = rules.setdefault(
            rid, {"rule_id": rid, "severity": e["severity"], "success": True, "details": []}
        )
        entry["success"] = entry["success"] and e["success"]
        entry["details"].append(e)

    failed = [r for r in rules.values() if not r["success"]]
    summary = {
        "suite": suite_name,
        "batch_id": batch_id,
        "executed_at": datetime.utcnow().isoformat() + "Z",
        "all_passed": len(failed) == 0,
        "critical_passed": not any(r["severity"] == "Critical" for r in failed),
        "failed_critical": [r["rule_id"] for r in failed if r["severity"] == "Critical"],
        "failed_warning": [r["rule_id"] for r in failed if r["severity"] == "Warning"],
        "failed_informational": [r["rule_id"] for r in failed if r["severity"] == "Informational"],
        "rules": sorted(rules.values(), key=lambda r: str(r["rule_id"])),
    }

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    out = EVIDENCE_DIR / f"{batch_id}__{suite_name}.json"
    out.write_text(json.dumps(summary, indent=2, default=str))
    summary["evidence_path"] = str(out)
    return summary


# ---------------------------------------------------------------------
# SPOTIFY RAW SUITE (gate: validate_spotify_raw) - DQ1..DQ5
# ---------------------------------------------------------------------

def _spotify_raw_expectations():
    exps = [
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="duration_ms", min_value=0, strict_min=True, mostly=0.999,
            meta={"rule_id": "DQ1", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="popularity", min_value=0, max_value=100,
            meta={"rule_id": "DQ2", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="track_name", mostly=0.999,
            meta={"rule_id": "DQ3", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="artists", mostly=0.999,
            meta={"rule_id": "DQ3", "severity": "Critical"},
        ),
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["track_id", "track_genre"],
            meta={"rule_id": "DQ5", "severity": "Warning"},
        ),
    ]
    for col in AUDIO_FEATURES_0_1:
        exps.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=0, max_value=1,
                meta={"rule_id": "DQ4", "severity": "Critical"},
            )
        )
    return exps


def run_spotify_raw_validation(df, batch_id: str = "manual") -> dict:
    context = get_context()
    checkpoint = _build_checkpoint(
        context, "spotify_raw", "spotify_raw_asset", _spotify_raw_expectations()
    )
    results = checkpoint.run(batch_parameters={"dataframe": df})
    return summarize_results(results, "spotify_raw_suite", batch_id)


# ---------------------------------------------------------------------
# GRAMMY RAW SUITE (gate: validate_grammys_raw) - DQ7, DQ8
# ---------------------------------------------------------------------

def add_winner_reliability_flag(df):
    """
    DQ8 support: a (year, category) group's 'winner' flag is only trustworthy
    when the group has exactly one row. Validation-support column computed
    just before the gate; it changes no business value.
    """
    df = df.copy()
    sizes = df.groupby(["year", "category"])["year"].transform("size")
    df["winner_flag_reliable"] = sizes == 1
    return df


def _grammy_raw_expectations():
    return [
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="year", min_value=1958, max_value=CURRENT_YEAR,
            meta={"rule_id": "DQ7", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="winner_flag_reliable", value_set=[True], mostly=0.70,
            meta={"rule_id": "DQ8", "severity": "Warning"},
        ),
    ]


def run_grammy_raw_validation(df, batch_id: str = "manual") -> dict:
    df = add_winner_reliability_flag(df)
    context = get_context()
    checkpoint = _build_checkpoint(
        context, "grammy_raw", "grammy_raw_asset", _grammy_raw_expectations()
    )
    results = checkpoint.run(batch_parameters={"dataframe": df})
    return summarize_results(results, "grammy_raw_suite", batch_id)


# ---------------------------------------------------------------------
# PREPARED GATE (validate_prepared) - DQ6, DQ9, DQ10, DQ11, DQ12, DQ13, DQ14
# One gate, two suites (one per prepared table), one combined summary.
# These are expected post-transformation invariants: they protect the
# dimensional load (a Critical failure here blocks load_dw).
# ---------------------------------------------------------------------

TRACKS_REQUIRED_COLUMNS = [
    "track_id", "track_name", "artists", "album_name", "explicit", "track_genre",
    "popularity", "duration_ms", "danceability", "energy", "valence", "tempo",
    "acousticness", "instrumentalness", "liveness", "speechiness",
    "loudness", "key", "mode", "time_signature", "is_grammy_nominated",
]

NOMINATIONS_REQUIRED_COLUMNS = [
    "grammy_row_id", "year", "decade", "category_raw", "category_normalized",
    "nominee", "artist", "winner_raw", "is_confirmed_winner",
    "matched_track_id", "match_type", "match_candidates", "match_ambiguous",
    "is_matched_to_spotify", "is_matched_to_spotify_int",
]


def add_prepared_support_columns(tracks_df, noms_df):
    """
    Validation-support columns (no business value changes):
      fk_valid                   matched_track_id is null or exists in tracks_prepared
      category_label_consistent  one canonical label per lower-cased category
    """
    noms = noms_df.copy()
    ids = set(tracks_df["track_id"])
    noms["fk_valid"] = noms["matched_track_id"].isna() | noms["matched_track_id"].isin(ids)
    key = noms["category_normalized"].str.lower().str.strip()
    noms["category_label_consistent"] = (
        noms.groupby(key)["category_normalized"].transform("nunique") == 1
    )
    return noms


def _prepared_tracks_expectations():
    return [
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["track_id", "track_genre"],
            meta={"rule_id": "DQ11", "severity": "Critical"},
        ),
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=TRACKS_REQUIRED_COLUMNS, exact_match=False,
            meta={"rule_id": "DQ13", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="duration_ms", min_value=0, strict_min=True,
            meta={"rule_id": "DQ14", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="track_name", meta={"rule_id": "DQ14", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="artists", meta={"rule_id": "DQ14", "severity": "Critical"},
        ),
    ]


def _prepared_nominations_expectations():
    return [
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="category_normalized",
            meta={"rule_id": "DQ6", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="category_label_consistent", value_set=[True],
            meta={"rule_id": "DQ6", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="grammy_row_id",
            meta={"rule_id": "DQ10", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="fk_valid", value_set=[True],
            meta={"rule_id": "DQ12", "severity": "Critical"},
        ),
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=NOMINATIONS_REQUIRED_COLUMNS, exact_match=False,
            meta={"rule_id": "DQ13", "severity": "Critical"},
        ),
        gx.expectations.ExpectColumnMeanToBeBetween(
            column="is_matched_to_spotify_int", min_value=0.04, max_value=0.15,
            meta={"rule_id": "DQ9", "severity": "Informational"},
        ),
    ]


def run_prepared_validation(tracks_df, nominations_df, batch_id: str = "manual") -> dict:
    noms = add_prepared_support_columns(tracks_df, nominations_df)
    context = get_context()
    cp_tracks = _build_checkpoint(
        context, "prepared_tracks", "prepared_tracks_asset", _prepared_tracks_expectations()
    )
    cp_noms = _build_checkpoint(
        context, "prepared_nominations", "prepared_nominations_asset",
        _prepared_nominations_expectations(),
    )
    r_tracks = cp_tracks.run(batch_parameters={"dataframe": tracks_df})
    r_noms = cp_noms.run(batch_parameters={"dataframe": noms})
    return summarize_results([r_tracks, r_noms], "prepared_suite", batch_id)


# ---------------------------------------------------------------------
# Rule -> Expectation traceability (README / docs/quality_rules.md)
# ---------------------------------------------------------------------

RULE_TO_EXPECTATION = {
    "DQ1":  ("spotify_raw_suite", "ExpectColumnValuesToBeBetween(duration_ms, strict_min=0, mostly=0.999)"),
    "DQ2":  ("spotify_raw_suite", "ExpectColumnValuesToBeBetween(popularity, 0..100)"),
    "DQ3":  ("spotify_raw_suite", "ExpectColumnValuesToNotBeNull(track_name, artists; mostly=0.999)"),
    "DQ4":  ("spotify_raw_suite", "ExpectColumnValuesToBeBetween(7 audio features, 0..1)"),
    "DQ5":  ("spotify_raw_suite", "ExpectCompoundColumnsToBeUnique(track_id, track_genre)"),
    "DQ6":  ("prepared_nominations", "ExpectColumnValuesToNotBeNull(category_normalized) + ExpectColumnValuesToBeInSet(category_label_consistent)"),
    "DQ7":  ("grammy_raw_suite", "ExpectColumnValuesToBeBetween(year, 1958..2026)"),
    "DQ8":  ("grammy_raw_suite", "ExpectColumnValuesToBeInSet(winner_flag_reliable, mostly=0.70)"),
    "DQ9":  ("prepared_nominations", "ExpectColumnMeanToBeBetween(is_matched_to_spotify_int, 0.04..0.15)"),
    "DQ10": ("prepared_nominations", "ExpectColumnValuesToBeUnique(grammy_row_id)"),
    "DQ11": ("prepared_tracks", "ExpectCompoundColumnsToBeUnique(track_id, track_genre)"),
    "DQ12": ("prepared_nominations", "ExpectColumnValuesToBeInSet(fk_valid, [True])"),
    "DQ13": ("prepared_tracks / prepared_nominations", "ExpectTableColumnsToMatchSet(required columns)"),
    "DQ14": ("prepared_tracks", "duration_ms > 0 and track_name/artists not null after quarantine"),
}
