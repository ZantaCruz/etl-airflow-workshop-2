# Transformation and Integration Decision Record (Section 6.8)

Implemented in `src/transform.py` (`transform_and_integrate`). Every rule is defensible
independently of any validation result: none of them exists only to make a gate pass.

## Transformation decisions

| ID | Rule and rationale | Affected fields | Before / after | Exception handling | Analytical impact |
|---|---|---|---|---|---|
| T1 | Drop the `Unnamed: 0` column, an export artifact (a pandas row index) with no meaning. | Spotify `Unnamed: 0` | 21 -> 20 columns | None needed | None |
| T2 | Quarantine rows that cannot be used safely: `duration_ms <= 0` or null `track_name` / `artists` (profiling evidence, DQ1 / DQ3). Rows are kept in `quarantine_spotify.parquet` with a reason; they are never silently lost. | Spotify `duration_ms`, `track_name`, `artists` | 114,000 rows -> 1 row quarantined | The row is excluded from `FactTrack` and recorded in the reconciliation JSON | Match keys are required by R1-R3; one row out of 114,000 does not move any aggregate |
| T3 | Deduplicate `(track_id, track_genre)`, keeping the highest popularity (DQ5: the same pair appears twice in 450 rows). A track under several different genres is NOT a duplicate and is kept. | Spotify `track_id`, `track_genre`, `popularity` | 113,999 -> 113,549 rows | Deterministic: sort by popularity desc, `track_id` asc, keep first | Without it, genre counts (R3) and averages (R1) double count |
| T4 | Normalize Grammy category labels (case and spacing) to one canonical label per category: the most frequent spelling (DQ6). | `category` -> `category_normalized` (raw kept in `category_raw`) | 638 labels -> 629 | Ties broken alphabetically | R3 groups by category; unnormalized labels would split one category into several |
| T5 | Derive `is_confirmed_winner`: `winner` is only trustworthy for a `(year, category)` group with exactly one row (DQ8; the flag is `True` on every row, including nominees, from 2011 on). `winner_raw` is preserved untouched. | `winner` -> `winner_raw`, `is_confirmed_winner` | 4,315 of 4,810 rows confirmed | Groups with several rows get `False`: "winner unknown", not "lost" | Not used by R1-R3; protects any later winner-based analysis |
| T6 | Integrate Grammy with Spotify (see the contract below). | `matched_track_id`, `match_type`, `match_candidates`, `match_ambiguous`, `is_matched_to_spotify` | 4,810 rows -> 4,810 rows (no row loss, no fan-out) | Null keys never match; unmatched rows are kept with `match_type = none` | R1, R2, R3 all depend on this link |
| T7 | Keep only the Grammy columns the model needs; `title`, `published_at`, `updated_at`, `workers`, `img` are not required by R1-R3. | Grammy columns | 11 -> 15 prepared columns (6 carried from the source, 9 derived) | Source table keeps everything, so nothing is lost | None |

## Integration contract

| Item | Team decision and evidence |
|---|---|
| Integration keys | Spotify `track_name` + `artists` <-> Grammy `nominee` + `artist`. Prepared matching keys are text-normalized (accents removed, lower case). |
| Matching strategy | Two tiers, recorded in `match_type`. **exact**: normalized title and full artist string are equal. **normalized**: title cleaned of `(...)`, `[...]` and a `" - suffix"` (e.g. `- Remastered`), plus the PRIMARY artist (Spotify: first before `;`; Grammy: first before `featuring`, `&`, `and`, `with`, `,`). Exact is tried first. |
| Cardinality | Before candidate selection the relationship is many-to-many (a Grammy nomination can match several Spotify tracks; a Spotify track appears under several genres). After selection: **each nomination maps to at most one track** (many-to-one), and a track can have several nominations (231 tracks carry 263 nominations). Verified by the invariant `grammy_row_id` unique and `rows_in == rows_prepared`. |
| Preprocessing | Accent stripping, lower casing, whitespace and punctuation collapsing; removal of version suffixes and parentheticals in the normalized tier; primary-artist extraction. |
| Unmatched records | Measured per match type in the reconciliation JSON: **4,547 of 4,810 (94.5%)** unmatched. Treatment: kept in `FactNomination` with a NULL track key. Causes: 1,840 rows have a null `artist` and can never match; most Grammy categories (albums, producers, composers, classical works) are not tracks. |
| Duplicate matches | Detected through `match_candidates`. **79** matched nominations have more than one candidate track. Resolution: the highest-popularity track wins (ties: lowest `track_id`), and the row is flagged `match_ambiguous = True`. A naive join on the same keys would turn 4,810 rows into 5,000 (+190), so candidate selection is mandatory; gate rule DQ10 blocks the load if it ever fails. |
| Assumptions and limitations | See below. |

### What the integration can and cannot establish reliably

- **It can** link a Grammy nominee to a Spotify track when title and primary artist agree, with an explicit confidence level (`match_type`) and an ambiguity flag.
- **It cannot** prove that two records are the same recording. The normalized tier can confuse a song with a remix, live or remastered version (e.g. `Rather Be` vs `Rather Be - The Magician Remix`); analyses that need high precision should filter `match_type = 'exact'` (181 rows). Precision was spot-checked manually on a sample, not measured against ground truth.
- **It cannot** align time. Spotify has no release date and its popularity is a current snapshot, while Grammy `year` is the award year. R2 therefore compares today's popularity of nominated tracks, not their popularity when nominated.
- **It cannot** match the 38% of Grammy rows without an artist.
- A nominated track under several genres appears once per genre in `FactTrack`; genre-level counts of nominations (R3) must count each nomination once per genre tag of its track and say so.

## Reconciliation evidence (real batch, `results/reconciliation/test_success__reconciliation.json`)

| Check | Result |
|---|---|
| Spotify: rows in = prepared + quarantined + duplicates removed | 114,000 = 113,549 + 1 + 450 |
| Spotify distinct tracks / tracks with a nomination | 89,740 / 231 |
| Grammy: rows in = rows prepared | 4,810 = 4,810 |
| Grammy `grammy_row_id` unique (no fan-out) | yes |
| Matches: exact / normalized / none | 181 / 82 / 4,547 |
| Overall match rate (per distinct nomination) | 5.47% |
| Ambiguous matches | 79 |
| Confirmed winners | 4,315 |

A violation of either row-count invariant raises an error in the task: it is a deterministic
failure and is not retried.
