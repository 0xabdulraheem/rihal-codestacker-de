import os
import logging
import psycopg2
from contextlib import contextmanager

logger = logging.getLogger(__name__)


def get_connection_params():
    return {
        "host": os.environ.get("PIPELINE_DB_HOST", "postgres"),
        "database": os.environ.get("PIPELINE_DB_NAME", "airflow"),
        "user": os.environ.get("PIPELINE_DB_USER", "airflow"),
        "password": os.environ.get("PIPELINE_DB_PASSWORD", "airflow"),
        "port": int(os.environ.get("PIPELINE_DB_PORT", "5432")),
        "connect_timeout": 10,
    }


@contextmanager
def get_connection():
    params = get_connection_params()
    conn = psycopg2.connect(**params)
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_cursor(conn):
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
