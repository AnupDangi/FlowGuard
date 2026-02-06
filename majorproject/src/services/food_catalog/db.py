"""Database connection and session management."""
import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool


class DatabaseConfig:
    """PostgreSQL configuration."""
    
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.user = os.getenv("POSTGRES_USER", "flowguard")
        self.password = os.getenv("POSTGRES_PASSWORD", "flowguard123")
        self.database = os.getenv("POSTGRES_DB", "food_catalog")
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_connection_params(self) -> dict:
        """Get connection parameters as dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }


# Global connection pool
_connection_pool: SimpleConnectionPool | None = None


def init_connection_pool(minconn: int = 1, maxconn: int = 10) -> None:
    """Initialize the connection pool."""
    global _connection_pool
    
    config = DatabaseConfig()
    _connection_pool = SimpleConnectionPool(
        minconn,
        maxconn,
        **config.get_connection_params()
    )
    print(f"✅ Database connection pool initialized ({minconn}-{maxconn} connections)")


def close_connection_pool() -> None:
    """Close all connections in the pool."""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        print("✅ Database connection pool closed")


@contextmanager
def get_db_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Get a database connection from the pool."""
    if _connection_pool is None:
        raise RuntimeError("Connection pool not initialized. Call init_connection_pool() first.")
    
    conn = _connection_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        _connection_pool.putconn(conn)


@contextmanager
def get_db_cursor(cursor_factory=RealDictCursor) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Get a database cursor with dict results."""
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
        finally:
            cursor.close()
