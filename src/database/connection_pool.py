import logging
import os
import queue
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Generator, Optional, TypeVar

from src.database.config import DatabaseConfig

logger = logging.getLogger("ecommerce-db-pool")
T = TypeVar("T")


class SQLiteConnectionPool:
    """
    Thread-safe connection pool for SQLite databases.
    Enables Write-Ahead Logging (WAL) mode for concurrent multi-reader, single-writer workloads,
    with automatic database file creation, timeout configuration, and retry error handling.
    """

    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.db_path = self.config.get_absolute_db_path()
        self.pool_size = self.config.pool_size
        self.timeout = self.config.timeout_seconds

        # Ensure parent directory and database exist
        self._ensure_database_exists()

        self._pool: queue.Queue[sqlite3.Connection] = queue.Queue(maxsize=self.pool_size)
        self._lock = threading.Lock()
        self._closed = False
        self._all_connections: list[sqlite3.Connection] = []

        # Pre-populate the connection pool
        for _ in range(self.pool_size):
            conn = self._create_connection()
            self._all_connections.append(conn)
            self._pool.put(conn)

        logger.info(
            "SQLiteConnectionPool initialized: db=%s, pool_size=%d, timeout=%.1fs",
            self.db_path,
            self.pool_size,
            self.timeout,
        )

    def _ensure_database_exists(self) -> None:
        """Automatically create the database directory and empty file if missing."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.db_path.exists():
            logger.info("Database file does not exist. Creating new database at %s", self.db_path)
            conn = sqlite3.connect(str(self.db_path))
            conn.close()

    def _create_connection(self) -> sqlite3.Connection:
        """Create a single tuned sqlite3.Connection instance with WAL pragmas."""
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=self.timeout,
            check_same_thread=False,
            isolation_level=None,  # We manage explicit transactions (autocommit off during tx)
        )
        conn.row_factory = sqlite3.Row

        # Tune SQLite pragmas for high-concurrency streaming pipelines
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode = WAL;")
        cursor.execute("PRAGMA synchronous = NORMAL;")
        cursor.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1000)};")
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.execute("PRAGMA cache_size = -64000;")  # 64MB memory cache
        cursor.close()
        return conn

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Checkout a connection from the pool, start a transaction, yield it,
        and commit on clean exit or rollback on error before checking back in.
        """
        if self._closed:
            raise RuntimeError("Cannot checkout connection from closed SQLiteConnectionPool.")

        conn = self._pool.get(block=True, timeout=self.timeout)
        try:
            conn.execute("BEGIN TRANSACTION;")
            yield conn
            conn.execute("COMMIT;")
        except Exception as exc:
            try:
                conn.execute("ROLLBACK;")
            except Exception as rollback_err:
                logger.warning("Rollback error: %s", rollback_err)
            logger.error("Transaction failed and was rolled back: %s", exc)
            raise
        finally:
            if not self._closed:
                self._pool.put(conn)

    def execute_with_retry(
        self,
        func: Callable[[sqlite3.Connection], T],
        max_retries: int = 4,
        base_delay: float = 0.1,
    ) -> T:
        """
        Executes a database function with exponential backoff if a lock/busy error occurs.
        """
        last_exception: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                with self.get_connection() as conn:
                    return func(conn)
            except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
                last_exception = exc
                err_msg = str(exc).lower()
                if "locked" in err_msg or "busy" in err_msg:
                    sleep_time = base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "Database locked on attempt %d/%d. Backing off for %.2fs: %s",
                        attempt,
                        max_retries,
                        sleep_time,
                        exc,
                    )
                    time.sleep(sleep_time)
                else:
                    raise
        raise RuntimeError(f"Failed after {max_retries} attempts: {last_exception}") from last_exception

    def close(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

            while not self._pool.empty():
                try:
                    self._pool.get_nowait()
                except queue.Empty:
                    break

            for conn in self._all_connections:
                try:
                    conn.close()
                except Exception as exc:
                    logger.debug("Error closing connection: %s", exc)

            logger.info("SQLiteConnectionPool successfully closed.")
