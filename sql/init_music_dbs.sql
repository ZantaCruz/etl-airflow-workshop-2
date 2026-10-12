-- init_music_dbs.sql
-- Runs automatically the first time the music_postgres container starts
-- (via /docker-entrypoint-initdb.d/). POSTGRES_DB already creates
-- grammy_source as the default database; this script additionally
-- creates music_dw (the final analytical Data Warehouse), so both
-- live in the same PostgreSQL instance but are logically separate
-- databases, matching the distinction the workshop requires between
-- "source preparation" (Grammy CSV -> grammy_source) and the final
-- "ETL Load" (validated prepared data -> music_dw).

CREATE DATABASE music_dw;
