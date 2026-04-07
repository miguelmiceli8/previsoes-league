"""Database connection module using PostgreSQL and environment variables."""

import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Create and return a new database connection.

    Supports two modes:
    - DATABASE_URL (Neon, Railway, Render, Streamlit Secrets)
    - Individual DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME vars (local)
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url, sslmode="require")

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        dbname=os.getenv("DB_NAME", "football_prediction"),
    )


@contextmanager
def get_cursor(dict_cursor: bool = True) -> Generator:
    """Context manager that yields a database cursor and handles commit/rollback.

    Args:
        dict_cursor: If True, returns a RealDictCursor for dict-like row access.
    """
    conn = get_connection()
    cursor_factory = RealDictCursor if dict_cursor else None
    cursor = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def init_database() -> None:
    """Initialize the database by executing the create.sql script."""
    sql_path = os.path.join(os.path.dirname(__file__), "create.sql")
    with open(sql_path, "r") as f:
        sql = f.read()

    with get_cursor(dict_cursor=False) as cursor:
        cursor.execute(sql)

    print("[DB] Database initialized successfully.")


def test_connection() -> bool:
    """Test database connectivity."""
    try:
        conn = get_connection()
        conn.close()
        print("[DB] Connection successful.")
        return True
    except Exception as e:
        print(f"[DB] Connection failed: {e}")
        return False


if __name__ == "__main__":
    if test_connection():
        init_database()