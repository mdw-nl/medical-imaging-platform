"""Thread-safe PostgreSQL interface with automatic reconnection."""

import contextlib
import logging
import threading
from pathlib import Path
from time import sleep

import psycopg2
import psycopg2.extensions

logger = logging.getLogger(__name__)


class PostgresInterface:
    """Thread-safe wrapper around psycopg2 with retry and reconnect logic."""

    @classmethod
    def from_settings(cls, settings, **kwargs) -> "PostgresInterface":
        """Create an unconnected instance from a ``PostgresSettings`` object."""
        return cls(
            host=settings.host,
            database=settings.db,
            user=settings.username,
            password=settings.password,
            port=settings.port,
            sslmode=getattr(settings, "sslmode", "prefer"),
            **kwargs,
        )

    @classmethod
    def connect_from_yaml(cls, config_path: Path, **kwargs) -> "PostgresInterface":
        """Create a connected instance by loading settings from a YAML file."""
        from imaging_common.config import load_settings  # noqa: PLC0415 — deferred to avoid circular import

        settings = load_settings(config_path)
        db = cls.from_settings(settings.postgres, **kwargs)
        db.connect()
        return db

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int | str = 5432,
        sslmode: str = "prefer",
        retry_attempts: int = 12,
        retry_delay: int = 2,
        max_retry_delay: int = 30,
    ):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = int(port)
        self.sslmode = sslmode
        self.conn = None
        self.cursor = None
        self._lock = threading.Lock()
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay
        self._max_retry_delay = max_retry_delay

    def _is_connected(self) -> bool:
        if self.conn is None or self.conn.closed:
            return False
        try:
            self.conn.isolation_level  # noqa: B018 — lightweight liveness check
            return True
        except psycopg2.OperationalError:
            return False

    def _ensure_connection(self):
        if not self._is_connected():
            logger.warning("Database connection lost, reconnecting...")
            self.connect()

    def connect(self):
        """Establish a database connection, retrying with exponential backoff on failure."""
        if self.conn and not self.conn.closed:
            with contextlib.suppress(Exception):
                self.conn.close()
        delay = self._retry_delay
        for attempt in range(self._retry_attempts):
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    sslmode=self.sslmode,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                self.conn.set_session(autocommit=True)
                self.cursor = self.conn.cursor()
                logger.info("Connection established.")
                return
            except psycopg2.OperationalError as e:
                if attempt < self._retry_attempts - 1:
                    logger.warning("%s", e)
                    logger.info(
                        "Retrying in %s seconds (attempt %d/%d)...",
                        delay,
                        attempt + 1,
                        self._retry_attempts,
                    )
                    sleep(delay)
                    delay = min(delay * 2, self._max_retry_delay)
                else:
                    raise ConnectionError("Unable to connect to the database after retries.") from e

    def disconnect(self):
        """Close the cursor and connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Connection closed.")

    def execute_query(self, query: str, params: tuple | None = None):
        """Execute a SQL statement, reconnecting automatically on connection loss."""
        with self._lock:
            self._ensure_connection()
            try:
                self.cursor.execute(query, params)
            except psycopg2.IntegrityError as e:
                if "duplicate key" in str(e).lower():
                    logger.warning("Duplicate entry ignored: %s", e)
                else:
                    logger.exception("Integrity error")
                    raise
            except psycopg2.OperationalError:
                logger.warning("Connection error during execute, reconnecting...")
                self._ensure_connection()
                self.cursor.execute(query, params)
            except Exception:
                logger.exception("Error executing query")
                raise

    def fetch_all(self, query: str, params: tuple | None = None) -> list | None:
        """Execute a query and return all rows, or ``None`` on error."""
        with self._lock:
            self._ensure_connection()
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchall()
            except psycopg2.OperationalError:
                logger.warning("Connection error during fetch, reconnecting...")
                self._ensure_connection()
                try:
                    self.cursor.execute(query, params)
                    return self.cursor.fetchall()
                except psycopg2.Error:
                    logger.exception("Error fetching results after reconnect")
                    return None
            except psycopg2.Error:
                logger.exception("Error fetching results")
                return None

    def fetch_one(self, query: str, params: tuple | None = None) -> tuple | None:
        """Execute a query and return the first row, or ``None`` on error."""
        with self._lock:
            self._ensure_connection()
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchone()
            except psycopg2.OperationalError:
                logger.warning("Connection error during fetch, reconnecting...")
                self._ensure_connection()
                try:
                    self.cursor.execute(query, params)
                    return self.cursor.fetchone()
                except psycopg2.Error:
                    logger.exception("Error fetching result after reconnect")
                    return None
            except psycopg2.Error:
                logger.exception("Error fetching result")
                return None

    def create_table(self, table_name: str, columns: dict[str, str]):
        """Create a table if it does not already exist."""
        columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        self.execute_query(query)

    def insert(self, table_name: str, data: dict):
        """Insert a single row into the given table."""
        if not data:
            raise ValueError("No data provided for insert.")
        columns = list(data.keys())
        values = [data[col] for col in columns]
        columns_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
        self.execute_query(query, tuple(values))

    def update(self, table_name: str, data: dict, where_conditions: dict):
        """Update rows matching *where_conditions* with the values in *data*."""
        set_clause = ", ".join([f"{col} = %s" for col in data])
        where_clause = " AND ".join([f"{col} = %s" for col in where_conditions])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        self.execute_query(query, tuple(data.values()) + tuple(where_conditions.values()))

    def delete(self, table_name: str, where_conditions: dict):
        """Delete rows matching *where_conditions*."""
        where_clause = " AND ".join([f"{col} = %s" for col in where_conditions])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        self.execute_query(query, tuple(where_conditions.values()))

    def check_table_exists(self, table_name: str) -> bool:
        """Return ``True`` if a table with the given name exists in the public schema."""
        with self._lock:
            self._ensure_connection()
            self.cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                );
                """,
                (table_name,),
            )
            return self.cursor.fetchone()[0]
