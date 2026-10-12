-- source_setup.sql
-- Runs against the grammy_source database (NOT music_dw).
-- Creates the table that holds the Grammy Awards CSV exactly as provided,
-- so the DAG's extract_grammys task reads from this relational source,
-- not from the CSV file directly (per Section 4.3 of the workshop brief).

DROP TABLE IF EXISTS grammy_awards_raw;

CREATE TABLE grammy_awards_raw (
    row_id        SERIAL PRIMARY KEY,   -- surrogate, not part of the source data
    year          INTEGER,
    title         VARCHAR(255),
    published_at  TIMESTAMP,
    updated_at    TIMESTAMP,
    category      VARCHAR(255),
    nominee       VARCHAR(500),
    artist        VARCHAR(500),
    workers       TEXT,
    img           TEXT,
    winner        BOOLEAN
);
