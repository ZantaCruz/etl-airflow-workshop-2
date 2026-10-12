"""
db.py

Connection helpers for the project's own PostgreSQL instance (music_postgres),
which hosts two logically separate databases:
    grammy_source -> operational source (Grammy CSV loaded here)
    music_dw      -> final dimensional Data Warehouse

Inside Docker, MUSIC_DB_HOST/PORT are injected by docker-compose.yaml.
Outside Docker (running scripts from your machine) the defaults point to
localhost:5433, the port music_postgres is published on.
"""

import os

from sqlalchemy import create_engine


def _engine_for(db_name: str):
    host = os.environ.get("MUSIC_DB_HOST", "localhost")
    port = os.environ.get("MUSIC_DB_PORT", "5433")
    user = os.environ.get("MUSIC_DB_USER", "music_user")
    password = os.environ.get("MUSIC_DB_PASSWORD", "music_pass")
    if host.startswith("/"):  # Unix-socket directory (local/embedded PostgreSQL)
        url = f"postgresql+psycopg2://{user}:{password}@/{db_name}?host={host}"
    else:
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(url)


def get_source_engine():
    return _engine_for(os.environ.get("MUSIC_DB_SOURCE_NAME", "grammy_source"))


def get_dw_engine():
    return _engine_for(os.environ.get("MUSIC_DB_DW_NAME", "music_dw"))


def get_admin_engine():
    """Engine on the source database (it always exists). Used for maintenance
    statements such as CREATE DATABASE, which cannot run inside a transaction."""
    return _engine_for(os.environ.get("MUSIC_DB_SOURCE_NAME", "grammy_source"))
