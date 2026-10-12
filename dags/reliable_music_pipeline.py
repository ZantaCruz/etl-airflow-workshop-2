"""
reliable_music_pipeline.py

Workshop 2 - Reliable Batch Data Pipeline (Spotify CSV + Grammy Awards database).

Architecture (Section 5):

    extract_spotify -> validate_spotify_raw --+
                                              +--> transform_and_integrate
    extract_grammys -> validate_grammys_raw --+            |
                                                           v
                                                    validate_prepared
                                                           |
                                                           v
                                                        load_dw  ->  Data Warehouse

The DAG only communicates workflow structure. All processing logic lives in
src/ (extract, validation, transform, load) so it is reusable and testable.

Task interfaces (Section 6.11): tasks exchange compact metadata only (paths,
identifiers, row counts, rule ids). Bulk data lives in staging parquet files
under data/staging/<batch_id>/, never in Airflow's metadata store.

Dependencies:
  * transform_and_integrate needs BOTH raw branches to have passed their gate.
  * load_dw needs validate_prepared to have passed.
  A Critical rule failure raises AirflowFailException, so the downstream tasks
  are marked upstream_failed and never run.

Failure policy and retries (documented in README, "Failure and retry policy"):
  Deterministic data / contract failures  -> AirflowFailException: fail at once,
      NO retry (repeating reproduces the same result).
  Transient operational failures (database temporarily unreachable) -> normal
      exception: bounded retry, only on tasks that touch the network/database.

  task                    retries  why
  extract_spotify         1        file on a mounted volume: rare transient I/O
  extract_grammys         3        database connection: transient outage possible
  validate_*              0        deterministic gate
  transform_and_integrate 0        deterministic computation
  load_dw                 2        transient DB errors only. A retry is SAFE because
                                   the load is one atomic transaction (a failed
                                   attempt rolls back; rerun = same result)

Concurrency: max_active_runs=1. The load replaces the whole warehouse
(TRUNCATE + INSERT in one transaction); two overlapping runs would fight over it.

Trigger parameters (test hooks for Section 7, defaults leave the pipeline untouched):
  inject_spotify_fault   "spotify_duration_ms_zero" simulates a corrupted Spotify
                         batch (5% duration_ms = 0) -> Test B, controlled failure.
  inject_prepared_fault  "join_fanout" simulates an integration bug so the PREPARED
                         gate (DQ10, Critical) must block load_dw.
  inject_load_failure    "dims" / "fact_track" aborts the load midway -> shows the
                         rollback behaviour (Section 7.3).
"""

import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

try:  # location differs between Airflow 3.x builds
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # pragma: no cover
    from airflow.exceptions import AirflowFailException

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

log = logging.getLogger(__name__)


def _batch_id() -> str:
    """Batch identifier = the DAG run id, made filesystem-safe."""
    run_id = get_current_context()["run_id"]
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def _is_permanent_db_error(exc) -> bool:
    """Database errors that retrying cannot fix (wrong credentials, missing database)."""
    msg = str(getattr(exc, "orig", exc)).lower()
    return any(t in msg for t in ("does not exist", "password authentication failed"))


def _apply_gate_policy(gate: str, summary: dict) -> dict:
    """
    Severity policy (docs/quality_rules.md):
      Critical failed      -> stop (AirflowFailException, no retry)
      Warning failed       -> log + continue
      Informational failed -> log + continue
    Returns compact metadata only.
    """
    batch = summary["batch_id"]
    if summary["failed_warning"]:
        log.warning("[batch=%s] %s WARNING rules failed (pipeline continues): %s",
                    batch, gate, summary["failed_warning"])
    if summary["failed_informational"]:
        log.info("[batch=%s] %s INFORMATIONAL rules failed (recorded only): %s",
                 batch, gate, summary["failed_informational"])

    if not summary["critical_passed"]:
        msg = (f"[batch={batch}] {gate} BLOCKED by Critical rule(s) "
               f"{summary['failed_critical']}. Downstream tasks will not run. "
               f"Evidence: {summary['evidence_path']}")
        log.error(msg)
        raise AirflowFailException(msg)

    log.info("[batch=%s] %s PASSED (Critical rules ok). Warnings=%s. Evidence: %s",
             batch, gate, summary["failed_warning"], summary["evidence_path"])
    return {
        "gate": gate,
        "batch_id": batch,
        "critical_passed": True,
        "failed_warning": summary["failed_warning"],
        "failed_informational": summary["failed_informational"],
        "evidence_path": summary["evidence_path"],
    }


@dag(
    dag_id="reliable_music_pipeline",
    description="Spotify CSV + Grammy relational source -> validated dimensional Data Warehouse",
    schedule=None,                      # manual batch trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                  # the load replaces the whole DW
    dagrun_timeout=timedelta(hours=1),
    default_args={"owner": "data-engineering"},
    params={
        "inject_spotify_fault": Param(
            "none", type="string", enum=["none", "spotify_duration_ms_zero"],
            description="Test B hook: simulate a corrupted Spotify batch.",
        ),
        "inject_prepared_fault": Param(
            "none", type="string", enum=["none", "join_fanout"],
            description="Test hook: simulate a join fan-out so the PREPARED gate must block load_dw.",
        ),
        "inject_load_failure": Param(
            "none", type="string", enum=["none", "dims", "fact_track"],
            description="Section 7.3 hook: abort the DW load midway (shows rollback).",
        ),
    },
    tags=["workshop-2", "etl", "great-expectations"],
    doc_md=__doc__,
)
def reliable_music_pipeline():

    # ---------------- Spotify branch ----------------

    @task(task_id="extract_spotify", retries=1, retry_delay=timedelta(seconds=15))
    def extract_spotify_task() -> dict:
        from extract import extract_spotify

        batch = _batch_id()
        fault = get_current_context()["params"].get("inject_spotify_fault", "none")
        try:
            meta = extract_spotify(batch, fault=fault)
        except (FileNotFoundError, ValueError) as exc:  # deterministic: do not retry
            raise AirflowFailException(f"[batch={batch}] extract_spotify failed: {exc}") from exc
        log.info("[batch=%s] extract_spotify OK: %s rows from %s (fault=%s)",
                 batch, meta["row_count"], meta["source"], meta["fault_injected"])
        return meta

    @task(task_id="validate_spotify_raw", retries=0)
    def validate_spotify_raw_task(meta: dict) -> dict:
        import pandas as pd
        from validation import run_spotify_raw_validation

        df = pd.read_parquet(meta["path"])
        log.info("[batch=%s] validate_spotify_raw: validating %d rows", meta["batch_id"], len(df))
        summary = run_spotify_raw_validation(df, meta["batch_id"])
        return _apply_gate_policy("validate_spotify_raw", summary)

    # ---------------- Grammy branch ----------------

    @task(task_id="extract_grammys", retries=3, retry_delay=timedelta(seconds=30),
          retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=5))
    def extract_grammys_task() -> dict:
        from extract import extract_grammys

        from sqlalchemy.exc import OperationalError, ProgrammingError

        batch = _batch_id()
        try:
            meta = extract_grammys(batch)
        except ProgrammingError as exc:   # deterministic: source table missing
            raise AirflowFailException(
                f"[batch={batch}] Grammy source table not found. Run the source preparation first: "
                "docker compose exec airflow-scheduler python /opt/airflow/src/prepare_source.py. "
                f"Detail: {getattr(exc, 'orig', exc)}") from exc
        except OperationalError as exc:
            if _is_permanent_db_error(exc):  # deterministic: retrying cannot help
                raise AirflowFailException(f"[batch={batch}] extract_grammys: {getattr(exc, 'orig', exc)}") from exc
            raise                            # transient (DB down / network): retried
        log.info("[batch=%s] extract_grammys OK: %s rows from %s",
                 batch, meta["row_count"], meta["source"])
        return meta

    @task(task_id="validate_grammys_raw", retries=0)
    def validate_grammys_raw_task(meta: dict) -> dict:
        import pandas as pd
        from validation import run_grammy_raw_validation

        df = pd.read_parquet(meta["path"])
        log.info("[batch=%s] validate_grammys_raw: validating %d rows", meta["batch_id"], len(df))
        summary = run_grammy_raw_validation(df, meta["batch_id"])
        return _apply_gate_policy("validate_grammys_raw", summary)

    # ---------------- Convergence ----------------

    @task(task_id="transform_and_integrate", retries=0)
    def transform_and_integrate_task(spotify_meta: dict, grammy_meta: dict,
                                     spotify_gate: dict, grammy_gate: dict) -> dict:
        # spotify_gate / grammy_gate are received only to make the dependency
        # explicit: this task cannot start unless both raw gates passed.
        from transform import transform_and_integrate

        batch = spotify_meta["batch_id"]
        try:
            meta = transform_and_integrate(spotify_meta["path"], grammy_meta["path"], batch)
        except ValueError as exc:   # reconciliation invariant violated: deterministic
            raise AirflowFailException(f"[batch={batch}] reconciliation failed: {exc}") from exc
        log.info("[batch=%s] transform_and_integrate OK. Spotify=%s | Grammy=%s",
                 batch, meta["spotify"], meta["grammy"])
        if get_current_context()["params"].get("inject_prepared_fault", "none") == "join_fanout":
            from transform import inject_join_fanout
            n = inject_join_fanout(meta["nominations_path"])
            log.warning("[batch=%s] FAULT INJECTION ACTIVE (test hook): %d duplicated nominations "
                        "appended to simulate a join fan-out", batch, n)
        return meta

    @task(task_id="validate_prepared", retries=0)
    def validate_prepared_task(transform_meta: dict) -> dict:
        import pandas as pd
        from validation import run_prepared_validation

        tracks = pd.read_parquet(transform_meta["tracks_path"])
        noms = pd.read_parquet(transform_meta["nominations_path"])
        log.info("[batch=%s] validate_prepared: %d track rows, %d nominations",
                 transform_meta["batch_id"], len(tracks), len(noms))
        summary = run_prepared_validation(tracks, noms, transform_meta["batch_id"])
        return _apply_gate_policy("validate_prepared", summary)

    # ---------------- Load ----------------

    @task(task_id="load_dw", retries=2, retry_delay=timedelta(minutes=1))
    def load_dw_task(transform_meta: dict, prepared_gate: dict) -> dict:
        # prepared_gate makes the dependency explicit: no load unless it passed.
        from sqlalchemy.exc import OperationalError
        from load import load_dw

        batch = transform_meta["batch_id"]
        fail_after = get_current_context()["params"].get("inject_load_failure", "none")
        try:
            summary = load_dw(
                transform_meta["tracks_path"], transform_meta["nominations_path"], batch,
                fail_after=None if fail_after == "none" else fail_after,
            )
        except OperationalError as exc:
            if _is_permanent_db_error(exc):  # deterministic: retrying cannot help
                raise AirflowFailException(f"[batch={batch}] load_dw: {getattr(exc, 'orig', exc)}") from exc
            raise                      # transient: let Airflow retry (the load is atomic)
        except Exception as exc:       # deterministic: stop, DW already rolled back
            raise AirflowFailException(
                f"[batch={batch}] load_dw failed and was rolled back: {exc}") from exc
        log.info("[batch=%s] load_dw OK. Rows after load: %s", batch, summary["rows_after"])
        return summary

    # ---------------- Wiring ----------------

    sp_meta = extract_spotify_task()
    gr_meta = extract_grammys_task()
    sp_gate = validate_spotify_raw_task(sp_meta)
    gr_gate = validate_grammys_raw_task(gr_meta)
    integrated = transform_and_integrate_task(sp_meta, gr_meta, sp_gate, gr_gate)
    prepared_gate = validate_prepared_task(integrated)
    load_dw_task(integrated, prepared_gate)


reliable_music_pipeline()
