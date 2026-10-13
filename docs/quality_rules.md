# Data Quality Rules (Section 6.5) and GX Mapping (Section 6.6)

Each rule traces back to profiling evidence (`notebooks/data_profiling.ipynb`), an analytical
requirement (R1-R3) or an expected post-transformation invariant. Thresholds are engineering
decisions justified below, not values chosen so the current batch passes.

## Quality rules

## Quality rules

| Rule ID | Dataset / Layer | Attribute(s) | Quality Dimension | Quality Rule | Metric / Threshold | Threshold Justification | Severity | Related Requirement |
|---|---|---|---|---|---|---|---|---|
| DQ1 | Spotify / raw | `duration_ms` | Validity | Duration must be greater than 0 | Violation rate <= 0.1% | Absorbs isolated glitch (1 row found); blocks systemic issues. | Critical | R1, R2, R3 |
| DQ2 | Spotify / raw | `popularity` | Validity | Must be within [0, 100] | 0 violations | Hard definitional bound; any violation implies source schema change. | Critical | R2 |
| DQ3 | Spotify / raw | `track_name`, `artists` | Completeness | Match keys must not be null | Null rate <= 0.1% | Tolerates the 1 known bad row for quarantine; blocks systemic export drops. | Critical | R1, R2, R3 |
| DQ4 | Spotify / raw | 7 audio features | Validity | Must be within [0, 1] | 0 violations | Hard definitional bound for Spotify API metrics. | Critical | R1 |
| DQ5 | Spotify / raw | `track_id` + `track_genre` | Uniqueness | The pair must be unique. | 0 duplicate pairs | Source duplication expected; flags for visibility before T3 dedup. | Warning | R1, R3 |
| DQ6 | Prepared / nominations | `category_normalized` | Consistency | One canonical label per category | 0 violations | Protects `DimCategory` key integrity; unnormalized labels break the load. | Critical | R3 |
| DQ7 | Grammy / raw | `year` | Validity | Award year within [1958, 2026] | 0 violations | Hard temporal bounds for the available data scope. | Critical | R1, R2, R3 |
| DQ8 | Grammy / raw | `winner` | Consistency | Single winner per category/year | >= 70% valid groups | 89.7% observed; degradation below 70% makes the metric untrustworthy. | Warning | Profiling |
| DQ9 | Prepared / nominations | `is_matched_to_spotify`| Reasonableness | Match rate within expected band | 4% - 15% | 5.5% observed. Below 4% means normalized tier failed; above 15% means over-matching. | Info | R1, R2, R3 |
| DQ10| Prepared / nominations | `grammy_row_id` | Uniqueness | One prepared row per source nomination | 0 duplicates | Protects DW against join fan-out (prevents 4,810 rows inflating to 5,000). | Critical | R2, R3 |
| DQ11| Prepared / tracks | `track_id` + `track_genre`| Uniqueness | Pair unique after deduplication | 0 duplicates | Post-transformation invariant of DQ5. | Critical | R1, R3 |
| DQ12| Prepared / nominations | `matched_track_id` | Referential Integrity| Match exists in prepared tracks | 0 orphan keys | Ensures foreign key relationship between Fact tables and `dim_track`. | Critical | R2, R3 |
| DQ13| Prepared / both tables | required columns | Validity (schema) | Required DW columns present | 0 missing columns | Structural requirement for atomic load logic. | Critical | R1, R2, R3 |
| DQ14| Prepared / tracks | `duration`, `track_name` | Validity | No invalid duration or null keys | 0 violations | Post-quarantine invariant ensuring T2 worked correctly. | Critical | R1, R2, R3 |

## Threshold rationale

- **DQ1 / DQ3 (tolerance 0.1%).** Profiling found a single row in 114,000 (0.0009%) with
  `duration_ms = 0` and null `track_name` / `artists` (the same row). An isolated defect of that
  size does not move aggregates computed over >100k rows, and the row is quarantined by
  `transform_and_integrate`. 0.1% is about 100x the observed defect rate: it tolerates isolated
  glitches, but a systemic problem (a broken export, a unit change) trips it and stops the
  pipeline. A zero-tolerance Critical rule would block every run of the provided batch because
  of one row. **Revision note:** the first draft used zero tolerance; implementation testing
  showed that consequence, and the threshold was revised on the evidence above.
- **DQ2 / DQ4 / DQ7 (zero tolerance).** Hard definitional bounds; any violation means a schema
  or unit change at the source, not an isolated glitch.
- **DQ5 (Warning).** A source duplication that is surfaced but does not block; T3 removes it.
  GX flags both members of each duplicate pair (894 rows); 450 rows are removed.
- **DQ6 (Critical, revised from Warning).** `category_normalized` becomes the key of
  `DimCategory`; a null or inconsistent label would break the load, so it belongs in the
  prepared gate as Critical.
- **DQ8 (>= 70%).** Winner flags are reliable for 89.7% of rows. Below 70% the nominee-list
  format (where `winner` is always `True`) would have spread enough to make winner-based
  analysis untrustworthy. R1-R3 do not use `winner`, hence Warning.
- **DQ9 (4%-15%, revised from 5%-20%).** The first draft used a match rate that was inflated by
  join fan-out (rows were counted after the join). Counted per distinct nomination the rate is
  5.5% (3.8% with exact matching only). 4% is the exact-only level: below it the normalized tier
  has stopped contributing. 15% is about 3x the observed rate: above it over-matching is likely.
  Informational, because it signals an integration regression without making the load unsafe.
- **DQ10 - DQ14 (Critical).** Expected post-transformation invariants that protect the
  dimensional load. DQ10 is justified by evidence: the same keys joined naively turn 4,810
  nominations into 5,000 rows.

## Severity policy (pipeline action)

| Severity | Pipeline action |
|---|---|
| Critical | The validation task fails; the affected downstream path is blocked (`transform_and_integrate` for the raw gates, `load_dw` for the prepared gate). Not retried: the failure is deterministic. |
| Warning | Recorded in `results/validation/*.json` and logged; the pipeline continues. |
| Informational | Recorded for monitoring; never blocks. |

Severity is evaluated per rule from the expectation `meta`, not from GX's global `success`
(which is `False` when any expectation fails, Warning ones included).

## Rule -> Great Expectations mapping

| Rule ID | Suite | GX Expectation |
|---|---|---|
| DQ1 | `spotify_raw_suite` | `ExpectColumnValuesToBeBetween(duration_ms, min_value=0, strict_min=True, mostly=0.999)` |
| DQ2 | `spotify_raw_suite` | `ExpectColumnValuesToBeBetween(popularity, 0, 100)` |
| DQ3 | `spotify_raw_suite` | `ExpectColumnValuesToNotBeNull(track_name / artists, mostly=0.999)` |
| DQ4 | `spotify_raw_suite` | `ExpectColumnValuesToBeBetween(<7 audio features>, 0, 1)` |
| DQ5 | `spotify_raw_suite` | `ExpectCompoundColumnsToBeUnique(track_id, track_genre)` |
| DQ6 | `prepared_nominations_suite` | `ExpectColumnValuesToNotBeNull(category_normalized)` + `ExpectColumnValuesToBeInSet(category_label_consistent, [True])` |
| DQ7 | `grammy_raw_suite` | `ExpectColumnValuesToBeBetween(year, 1958, 2026)` |
| DQ8 | `grammy_raw_suite` | `ExpectColumnValuesToBeInSet(winner_flag_reliable, [True], mostly=0.70)` |
| DQ9 | `prepared_nominations_suite` | `ExpectColumnMeanToBeBetween(is_matched_to_spotify_int, 0.04, 0.15)` |
| DQ10 | `prepared_nominations_suite` | `ExpectColumnValuesToBeUnique(grammy_row_id)` |
| DQ11 | `prepared_tracks_suite` | `ExpectCompoundColumnsToBeUnique(track_id, track_genre)` |
| DQ12 | `prepared_nominations_suite` | `ExpectColumnValuesToBeInSet(fk_valid, [True])` |
| DQ13 | both prepared suites | `ExpectTableColumnsToMatchSet(<required columns>, exact_match=False)` |
| DQ14 | `prepared_tracks_suite` | `ExpectColumnValuesToBeBetween(duration_ms, strict_min=0)` + `ExpectColumnValuesToNotBeNull(track_name, artists)` |

`winner_flag_reliable`, `fk_valid` and `category_label_consistent` are validation-support columns
computed just before the gate; they change no business value.

## GX design elements

| Element | Implementation (`src/validation.py`) |
|---|---|
| Data object validated | The whole pandas DataFrame read from the batch's staging parquet |
| Expectation Suites | `spotify_raw_suite`, `grammy_raw_suite`, `prepared_tracks_suite`, `prepared_nominations_suite` |
| Validation Definitions | `<prefix>_validation`: suite + whole-DataFrame batch definition |
| Checkpoints | `<prefix>_checkpoint`, executed by `run_*_validation()` |
| Validation result | Per-rule JSON in `results/validation/<batch_id>__<suite>.json` (rule id, severity, unexpected count and percent) |

## Validation evidence (standalone runs; Airflow runs produce their own)

| Run | Gate | Outcome | Evidence file |
|---|---|---|---|
| Real batch | `validate_spotify_raw` | Critical rules pass; DQ5 (Warning) fails, pipeline continues | `results/validation/test_success__spotify_raw_suite.json` |
| Real batch | `validate_grammys_raw` | All rules pass | `results/validation/test_success__grammy_raw_suite.json` |
| Real batch | `validate_prepared` | All rules pass | `results/validation/test_success__prepared_suite.json` |
| Controlled failure: 5% `duration_ms = 0` | `validate_spotify_raw` | DQ1 fails (Critical), 5,701 rows (5.00%) | `results/validation/test_failure__spotify_raw_suite.json` |
| Controlled failure: join fan-out (190 duplicated nominations) | `validate_prepared` | DQ10 fails (Critical) | `results/validation/test_failure_fanout__prepared_suite.json` |
| Controlled failure: 5 orphan foreign keys | `validate_prepared` | DQ12 fails (Critical) | `results/validation/test_failure_fk__prepared_suite.json` |
