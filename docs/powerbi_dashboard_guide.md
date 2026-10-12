# Power BI dashboard guide (Section 8.1)

The dashboard answers R1-R3 using **only** the Data Warehouse (`music_dw`), through the five `kpi_*` views.
No CSV is connected. Power BI Desktop, Import mode.

## 0. Before opening Power BI: confirm the warehouse is ready

Run the DAG at least once with the current project version (the views are created by `load_dw`), then:

```bash
docker compose exec music_postgres psql -U music_user -d music_dw -c "SELECT viewname FROM pg_views WHERE schemaname = 'public' ORDER BY 1;"
docker compose exec music_postgres psql -U music_user -d music_dw -c "SELECT is_grammy_nominated, tracks, ROUND(avg_popularity::numeric,1) AS avg_popularity FROM kpi_r2_popularity_summary ORDER BY 1;"
```

Expected: five views (`kpi_r1_audio_profile`, `kpi_r1_audio_profile_overall`, `kpi_r2_popularity_bands`, `kpi_r2_popularity_summary`, `kpi_r3_genre_by_decade`)
and the rows `false | 89509 | 33.1` and `true | 231 | 59.1`.

## 1. Connect

Get Data -> PostgreSQL database.

| Field | Value |
|---|---|
| Server | `localhost:5433` (or your `MUSIC_DB_HOST_PORT`, e.g. `localhost:5434`) |
| Database | `music_dw` |
| Data connectivity mode | **Import** |
| User / password | `music_user` / `music_pass` |

If Power BI warns about encryption, accept connecting without it (local Docker, no SSL). In the Navigator tick the five `kpi_*` views and Load.
Tables may appear with a `public ` prefix.

## 2. Prepare the data (Transform data / Power Query)

1. **Friendly group label** on `kpi_r1_audio_profile_overall`, `kpi_r2_popularity_summary` and `kpi_r2_popularity_bands`: Add Column -> Custom Column,
   name `Group`, formula `if [is_grammy_nominated] = true then "Grammy-nominated" else "Rest of catalog"`.
2. **Unpivot for KPI-1** (so the four audio features become the X axis): in `kpi_r1_audio_profile_overall` select the columns `avg_danceability`, `avg_energy`,
   `avg_valence`, `avg_acousticness` -> right click -> **Unpivot Columns**. Rename `Attribute` to `Feature` and `Value` to `Average`. Optionally replace the
   text `avg_` with nothing in `Feature` (Transform -> Replace Values).
3. **Decade label** on `kpi_r3_genre_by_decade`: Custom Column `Decade`, formula `Text.From([decade]) & "s"` (a text axis avoids Power BI summing the decades).
4. Close & Apply.

## 3. Measure for KPI-2 (percentage within each group)

On `kpi_r2_popularity_bands` create a new measure:

```
Pct of group =
DIVIDE(
    SUM('public kpi_r2_popularity_bands'[tracks]),
    CALCULATE(SUM('public kpi_r2_popularity_bands'[tracks]), ALLEXCEPT('public kpi_r2_popularity_bands', 'public kpi_r2_popularity_bands'[Group]))
)
```

Adjust the table name if Power BI shows it differently (use the autocomplete). Format as percentage.

## 4. Visuals (one page)

| # | Requirement | Visual | Fields | Title |
|---|---|---|---|---|
| 1 | R1 | Clustered column | X axis `Feature`; Y axis `Average` (aggregation: **Average**); Legend `Group` | KPI-1 (R1): audio profile, nominated vs rest of catalog |
| 2 | R2 | Clustered column | X axis `popularity_band`; Y axis `Pct of group`; Legend `Group` | KPI-2 (R2): popularity distribution, nominated vs rest |
| 3 | R2 | Table | `Group`, `tracks`, `avg_popularity`, `median_popularity`, `share_popularity_50_plus` (from `kpi_r2_popularity_summary`) | KPI-2 (R2): popularity summary |
| 4 | R3 | Stacked column | X axis `Decade`; Y axis `nominations`; Legend `genre_name`; filter `genre_name` -> Top N = 7 by `nominations` | KPI-3 (R3): genres of nominated tracks by decade (top 7 genres) |

Add data labels to visuals 1 and 2. Keep `Rest of catalog` grey and `Grammy-nominated` blue in both. In the table set `share_popularity_50_plus` to percentage format.

## 5. Page layout and text

- Page title: **Spotify x Grammy: does recognition relate to sound, popularity and genre?**
- Subtitle text box: *Source: Data Warehouse `music_dw` (PostgreSQL), KPI views. Only nominations linked to a Spotify track are analysed (263 of 4,810; 231 distinct tracks).*
- Under each visual a one-line text box with the traceability and the reading:
  - R1: *Nominated songs are slightly happier (valence) and less acoustic; danceability, energy and tempo are similar. Indicative: 231 tracks.*
  - R2: *Nominated tracks are far more popular today (average 59.1 vs 33.1). Association, not causation; popularity is a current snapshot.*
  - R3: *Genres follow their era (disco 1970s-80s, dance/electro/pop 2010s). Small counts per cell; top 7 genres shown.*

## 6. Numbers your dashboard must show (use them to validate it)

| Check | Expected |
|---|---|
| KPI-1 rest of catalog: danceability / energy / valence / acousticness | 0.567 / 0.642 / 0.474 / 0.314 |
| KPI-1 nominated | 0.578 / 0.638 / 0.527 / 0.251 |
| KPI-2 table: tracks, average, median, share >= 50 (rest) | 89,509 / 33.1 / 33 / 23.5% |
| KPI-2 table (nominated) | 231 / 59.1 / 68 / 80.1% |
| KPI-3 largest cells | dance 2010s = 15; disco 1980s = 11 |

If a number differs, the cause is almost always an aggregation set to Sum instead of Average, or averaging averages across genres: use the `_overall` view for the headline.

## 7. Save the evidence

- Screenshot the full page and each visual (cropped): `docs/figures/powerbi_dashboard.png`, `powerbi_kpi1.png`, `powerbi_kpi2.png`, `powerbi_kpi3.png`.
- Save the report as `dashboard/music_dashboard.pbix` (small: the views are tiny).
- In the README, replace the reference renderings of Section 15 with these screenshots and register them as evidence E-U4.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Cannot connect | Check the port (`docker compose ps`), that `music_postgres` is healthy, and that the DAG has run (the views do not exist before the first load) |
| Authentication error | Send the exact message; it can depend on the PostgreSQL password method |
| Decades summed on the axis | Use the text `Decade` column, not `decade` |
| Genre legend too crowded | Keep the Top N = 7 filter |
| Numbers differ from section 6 | Aggregation must be Average (KPI-1); check that the filter pane has no leftover filters |
