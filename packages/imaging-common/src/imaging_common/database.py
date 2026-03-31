import logging
import threading
from time import sleep

import psycopg2

logger = logging.getLogger(__name__)


class PostgresInterface:
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int | str = 5432,
        retry_attempts: int = 5,
        retry_delay: int = 10,
    ):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = int(port)
        self.conn = None
        self.cursor = None
        self._lock = threading.Lock()
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay

    def connect(self):
        for attempt in range(self._retry_attempts):
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                self.cursor = self.conn.cursor()
                logger.info("Connection established.")
                return
            except psycopg2.OperationalError as e:
                if attempt < self._retry_attempts - 1:
                    logger.warning("%s", e)
                    logger.info("Retrying in %s seconds...", self._retry_delay)
                    sleep(self._retry_delay)
                else:
                    raise ConnectionError("Unable to connect to the database after retries.") from e

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Connection closed.")

    def execute_query(self, query: str, params: tuple | None = None):
        with self._lock:
            try:
                self.cursor.execute(query, params)
                self.conn.commit()
            except psycopg2.IntegrityError as e:
                self.conn.rollback()
                if "duplicate key" in str(e).lower():
                    logger.warning("Duplicate entry ignored: %s", e)
                else:
                    logger.exception("Integrity error")
                    raise
            except Exception:
                self.conn.rollback()
                logger.exception("Error executing query")
                raise

    def fetch_all(self, query: str, params: tuple | None = None) -> list | None:
        with self._lock:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchall()
            except Exception:
                logger.exception("Error fetching results")
                return None

    def fetch_one(self, query: str, params: tuple | None = None) -> tuple | None:
        with self._lock:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchone()
            except Exception:
                logger.exception("Error fetching result")
                return None

    def create_table(self, table_name: str, columns: dict[str, str]):
        columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        self.execute_query(query)

    def insert(self, table_name: str, data: dict):
        if not data:
            raise ValueError("No data provided for insert.")
        columns = list(data.keys())
        values = [data[col] for col in columns]
        columns_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
        self.execute_query(query, tuple(values))

    def update(self, table_name: str, data: dict, where_conditions: dict):
        set_clause = ", ".join([f"{col} = %s" for col in data])
        where_clause = " AND ".join([f"{col} = %s" for col in where_conditions])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        self.execute_query(query, tuple(data.values()) + tuple(where_conditions.values()))

    def delete(self, table_name: str, where_conditions: dict):
        where_clause = " AND ".join([f"{col} = %s" for col in where_conditions])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        self.execute_query(query, tuple(where_conditions.values()))

    def check_table_exists(self, table_name: str) -> bool:
        with self._lock:
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
