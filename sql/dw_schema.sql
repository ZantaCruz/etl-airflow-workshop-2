-- dw_schema.sql
-- Dimensional Data Warehouse (database music_dw): fact constellation with two
-- fact tables that share the dim_track dimension.
--
--   fact_track       grain: one row per (track, genre) entry of the Spotify catalog
--   fact_nomination  grain: one row per Grammy nomination (matched to a track or not)
--
-- Key strategy:
--   * dim_track, dim_genre, dim_category: integer surrogate keys assigned by the
--     load step in business-key order (deterministic: same input -> same keys),
--     with UNIQUE constraints on the business keys (track_id, genre_name,
--     category_name).
--   * dim_year: the business key (the award year) is the primary key.
--   * fact_nomination.nomination_id keeps the source row_id (business key).
--   * fact_nomination.track_key is NULLABLE by design: most nominations have no
--     Spotify match. The CHECK ties NULL <-> match_type = 'none'.
--
-- Idempotent: CREATE ... IF NOT EXISTS. Reruns never DROP; load.py replaces the
-- data atomically (TRUNCATE + INSERT in one transaction).

CREATE TABLE IF NOT EXISTS dim_track (
    track_key   INTEGER PRIMARY KEY,
    track_id    VARCHAR(32) NOT NULL UNIQUE,
    track_name  TEXT NOT NULL,
    artists     TEXT NOT NULL,
    album_name  TEXT,
    explicit    BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key   INTEGER PRIMARY KEY,
    genre_name  VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key   INTEGER PRIMARY KEY,
    category_name  VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_key  INTEGER PRIMARY KEY,
    decade    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_track (
    track_key            INTEGER NOT NULL REFERENCES dim_track(track_key),
    genre_key            INTEGER NOT NULL REFERENCES dim_genre(genre_key),
    popularity           SMALLINT NOT NULL CHECK (popularity BETWEEN 0 AND 100),
    duration_ms          INTEGER NOT NULL CHECK (duration_ms > 0),
    danceability         DOUBLE PRECISION NOT NULL CHECK (danceability BETWEEN 0 AND 1),
    energy               DOUBLE PRECISION NOT NULL CHECK (energy BETWEEN 0 AND 1),
    valence              DOUBLE PRECISION NOT NULL CHECK (valence BETWEEN 0 AND 1),
    acousticness         DOUBLE PRECISION NOT NULL CHECK (acousticness BETWEEN 0 AND 1),
    instrumentalness     DOUBLE PRECISION NOT NULL CHECK (instrumentalness BETWEEN 0 AND 1),
    liveness             DOUBLE PRECISION NOT NULL CHECK (liveness BETWEEN 0 AND 1),
    speechiness          DOUBLE PRECISION NOT NULL CHECK (speechiness BETWEEN 0 AND 1),
    tempo                DOUBLE PRECISION NOT NULL,
    loudness             DOUBLE PRECISION NOT NULL,
    musical_key          SMALLINT NOT NULL,
    mode                 SMALLINT NOT NULL,
    time_signature       SMALLINT NOT NULL,
    is_grammy_nominated  BOOLEAN NOT NULL,
    PRIMARY KEY (track_key, genre_key)
);

CREATE TABLE IF NOT EXISTS fact_nomination (
    nomination_id          INTEGER PRIMARY KEY,
    year_key               INTEGER NOT NULL REFERENCES dim_year(year_key),
    category_key           INTEGER NOT NULL REFERENCES dim_category(category_key),
    track_key              INTEGER REFERENCES dim_track(track_key),
    nominee                TEXT,
    artist                 TEXT,
    winner_raw             BOOLEAN NOT NULL,
    is_confirmed_winner    BOOLEAN NOT NULL,
    match_type             VARCHAR(10) NOT NULL CHECK (match_type IN ('exact', 'normalized', 'none')),
    match_candidates       SMALLINT NOT NULL,
    match_ambiguous        BOOLEAN NOT NULL,
    is_matched_to_spotify  BOOLEAN NOT NULL,
    CHECK ((track_key IS NULL) = (match_type = 'none'))
);

CREATE INDEX IF NOT EXISTS idx_fact_track_genre      ON fact_track(genre_key);
CREATE INDEX IF NOT EXISTS idx_fact_nomination_year  ON fact_nomination(year_key);
CREATE INDEX IF NOT EXISTS idx_fact_nomination_cat   ON fact_nomination(category_key);
CREATE INDEX IF NOT EXISTS idx_fact_nomination_track ON fact_nomination(track_key);

-- Load evidence: one row per load attempt (SUCCESS rows are written inside the
-- load transaction; FAILED rows are written after the rollback).
CREATE TABLE IF NOT EXISTS etl_load_audit (
    load_id    SERIAL PRIMARY KEY,
    batch_id   TEXT NOT NULL,
    loaded_at  TIMESTAMP NOT NULL DEFAULT now(),
    status     TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    summary    JSONB,
    error      TEXT
);

-- ---------------------------------------------------------------------
-- KPI views (Section 8.1). The dashboard queries these views, never the CSVs.
-- They are plain SELECTs over the dimensional tables, so they always reflect
-- the last successfully loaded snapshot.
-- ---------------------------------------------------------------------

-- R1: audio profile of nominated tracks vs the rest of the catalog, by genre.
-- Grain: genre x nominated flag. Averages are over track-genre rows.
CREATE OR REPLACE VIEW kpi_r1_audio_profile AS
SELECT g.genre_name,
       f.is_grammy_nominated,
       COUNT(*)                          AS track_genre_rows,
       AVG(f.danceability)               AS avg_danceability,
       AVG(f.energy)                     AS avg_energy,
       AVG(f.valence)                    AS avg_valence,
       AVG(f.acousticness)               AS avg_acousticness,
       AVG(f.tempo)                      AS avg_tempo
FROM fact_track f
JOIN dim_genre g ON g.genre_key = f.genre_key
GROUP BY g.genre_name, f.is_grammy_nominated;

-- R1 overall (all genres together): the headline comparison.
CREATE OR REPLACE VIEW kpi_r1_audio_profile_overall AS
SELECT f.is_grammy_nominated,
       COUNT(*)                          AS track_genre_rows,
       AVG(f.danceability)               AS avg_danceability,
       AVG(f.energy)                     AS avg_energy,
       AVG(f.valence)                    AS avg_valence,
       AVG(f.acousticness)               AS avg_acousticness,
       AVG(f.tempo)                      AS avg_tempo
FROM fact_track f
GROUP BY f.is_grammy_nominated;

-- R2: popularity of nominated vs non-nominated tracks. One popularity per TRACK
-- (a track appears once per genre tag; MAX avoids counting it several times).
CREATE OR REPLACE VIEW kpi_r2_popularity_summary AS
WITH per_track AS (
    SELECT track_key,
           MAX(popularity)           AS popularity,
           BOOL_OR(is_grammy_nominated) AS is_grammy_nominated
    FROM fact_track
    GROUP BY track_key
)
SELECT is_grammy_nominated,
       COUNT(*)                                                   AS tracks,
       AVG(popularity)                                            AS avg_popularity,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY popularity)    AS median_popularity,
       AVG(CASE WHEN popularity >= 50 THEN 1.0 ELSE 0.0 END)      AS share_popularity_50_plus
FROM per_track
GROUP BY is_grammy_nominated;

CREATE OR REPLACE VIEW kpi_r2_popularity_bands AS
WITH per_track AS (
    SELECT track_key,
           MAX(popularity)           AS popularity,
           BOOL_OR(is_grammy_nominated) AS is_grammy_nominated
    FROM fact_track
    GROUP BY track_key
)
SELECT is_grammy_nominated,
       CASE WHEN popularity < 20 THEN '00-19'
            WHEN popularity < 40 THEN '20-39'
            WHEN popularity < 60 THEN '40-59'
            WHEN popularity < 80 THEN '60-79'
            ELSE '80-100' END                                     AS popularity_band,
       COUNT(*)                                                   AS tracks
FROM per_track
GROUP BY is_grammy_nominated, popularity_band;

-- R3: genres of matched Grammy nominations over the decades. A nomination counts
-- once per genre tag of its track (a track can carry several genre tags).
CREATE OR REPLACE VIEW kpi_r3_genre_by_decade AS
SELECT y.decade,
       g.genre_name,
       COUNT(*) AS nominations
FROM fact_nomination n
JOIN dim_year  y  ON y.year_key  = n.year_key
JOIN fact_track ft ON ft.track_key = n.track_key
JOIN dim_genre g  ON g.genre_key = ft.genre_key
WHERE n.track_key IS NOT NULL
GROUP BY y.decade, g.genre_name;
