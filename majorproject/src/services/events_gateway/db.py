"""Database connection for Events Gateway service."""
import os
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


class DatabaseConfig:
    """Database configuration."""
    
    HOST = os.getenv("POSTGRES_HOST", "localhost")
    PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    DATABASE = os.getenv("POSTGRES_DB", "food_catalog")
    USER = os.getenv("POSTGRES_USER", "flowguard")
    PASSWORD = os.getenv("POSTGRES_PASSWORD", "flowguard123")


# Global connection pool
connection_pool: Optional[pool.SimpleConnectionPool] = None


def init_connection_pool():
    """Initialize database connection pool."""
    global connection_pool

    if connection_pool is not None:
        return  # Already initialized

    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            2,   # Min connections
            10,  # Max connections
            host=DatabaseConfig.HOST,
            port=DatabaseConfig.PORT,
            database=DatabaseConfig.DATABASE,
            user=DatabaseConfig.USER,
            password=DatabaseConfig.PASSWORD
        )
        print(f"✅ Database connection pool initialized (Events Gateway) "
              f"→ {DatabaseConfig.HOST}:{DatabaseConfig.PORT}/{DatabaseConfig.DATABASE}")
    except Exception as e:
        # Log the real error clearly — don't silently swallow it
        print(f"❌ Failed to initialize DB pool: {e}")
        print(f"   HOST={DatabaseConfig.HOST} PORT={DatabaseConfig.PORT} "
              f"DB={DatabaseConfig.DATABASE} USER={DatabaseConfig.USER}")
        # Re-raise so the caller knows startup failed
        raise


def close_connection_pool():
    """Close all database connections."""
    global connection_pool
    
    if connection_pool:
        connection_pool.closeall()
        connection_pool = None
        print("✅ Database connection pool closed")


@contextmanager
def get_db_connection():
    """Get database connection from pool.

    Lazy-initializes the pool on first use if startup failed.
    This means orders will work even if the DB wasn't ready when
    the gateway started.
    """
    global connection_pool
    if connection_pool is None:
        # Retry pool init (e.g. DB was down at startup but is up now)
        try:
            init_connection_pool()
        except Exception as e:
            raise RuntimeError(
                f"Database connection pool not initialized. "
                f"PostgreSQL may be down or misconfigured. "
                f"HOST={DatabaseConfig.HOST} PORT={DatabaseConfig.PORT}. "
                f"Error: {e}"
            )

    conn = connection_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        connection_pool.putconn(conn)


@contextmanager
def get_db_cursor():
    """Get database cursor (returns dict results)."""
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
        finally:
            cursor.close()
