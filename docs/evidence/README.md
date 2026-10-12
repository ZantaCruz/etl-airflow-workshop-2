# Evidence

`sandbox_reference_runs/` holds evidence produced while building the project (Airflow 3.1.8
`DAG.test()` + real PostgreSQL + real Great Expectations, no Docker UI). It documents expected
behaviour and gives you a reference to compare against.

**Your own evidence** (required by Sections 7 and 8.3) comes from running the DAG in your Docker Airflow:

| What | Where it is produced |
|---|---|
| DAG Graph / Grid view, task states (Test A, Test B, rerun) | Airflow UI -> screenshot (cropped, with a written interpretation) |
| Failed task log (Test B) | Airflow UI -> failed task -> Logs |
| GX validation results (JSON, per rule) | `results/validation/<run_id>__<suite>.json` (written on your machine, folder is mounted) |
| Reconciliation counts | `results/reconciliation/<run_id>__reconciliation.json` |
| DW load audit and row counts | SQL on `music_dw` (see the README, "Verify the Data Warehouse") |

Register each piece in the Reliability Evidence Register (README) with: Evidence ID, Run/Task, Path, What it proves, Related rule.
