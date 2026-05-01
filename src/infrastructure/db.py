from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from functools import cache
from queue import Empty, LifoQueue

import psycopg

DB_BOOTSTRAP_SCHEMA_ENV = "DB_BOOTSTRAP_SCHEMA"
DB_POOL_MAX_SIZE_ENV = "DB_POOL_MAX_SIZE"
_DEFAULT_DB_POOL_MAX_SIZE = 4
_SCHEMA_STATE_TABLE = "ops.schema_state"


class DatabaseSchemaError(RuntimeError):
    """Raised when the database schema is missing or incompatible."""


def env_flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def database_schema_bootstrap_enabled() -> bool:
    return env_flag(DB_BOOTSTRAP_SCHEMA_ENV)


def database_pool_max_size() -> int:
    raw_value = os.getenv(DB_POOL_MAX_SIZE_ENV, str(_DEFAULT_DB_POOL_MAX_SIZE)).strip()
    if not raw_value:
        return _DEFAULT_DB_POOL_MAX_SIZE

    value = int(raw_value)
    if value <= 0:
        raise ValueError(f"{DB_POOL_MAX_SIZE_ENV} must be a positive integer.")
    return value


class DatabaseConnectionPool:
    def __init__(self, database_url: str, *, max_size: int) -> None:
        self.database_url = database_url
        self.max_size = max(1, max_size)
        self._available: LifoQueue[psycopg.Connection] = LifoQueue()
        self._created = 0
        self._lock = threading.Lock()

    def _open_connection(self) -> psycopg.Connection:
        return psycopg.connect(self.database_url)

    @staticmethod
    def _is_usable(connection: psycopg.Connection) -> bool:
        return not connection.closed and not bool(getattr(connection, "broken", False))

    def _discard_connection(self, connection: psycopg.Connection) -> None:
        try:
            connection.close()
        finally:
            with self._lock:
                self._created = max(0, self._created - 1)

    def _borrow_connection(self) -> psycopg.Connection:
        while True:
            try:
                connection = self._available.get_nowait()
            except Empty:
                break

            if self._is_usable(connection):
                return connection
            self._discard_connection(connection)

        with self._lock:
            if self._created < self.max_size:
                self._created += 1
                should_create = True
            else:
                should_create = False

        if should_create:
            try:
                return self._open_connection()
            except Exception:
                with self._lock:
                    self._created = max(0, self._created - 1)
                raise

        while True:
            connection = self._available.get()
            if self._is_usable(connection):
                return connection
            self._discard_connection(connection)

    def _return_connection(self, connection: psycopg.Connection) -> None:
        if self._is_usable(connection):
            self._available.put(connection)
            return
        self._discard_connection(connection)

    @staticmethod
    def _reset_connection(connection: psycopg.Connection) -> None:
        try:
            connection.rollback()
        except Exception:
            connection.close()

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        connection = self._borrow_connection()
        try:
            yield connection
        finally:
            self._reset_connection(connection)
            self._return_connection(connection)


@cache
def _cached_connection_pool(database_url: str, max_size: int) -> DatabaseConnectionPool:
    return DatabaseConnectionPool(database_url, max_size=max_size)


def get_connection_pool(database_url: str) -> DatabaseConnectionPool:
    return _cached_connection_pool(database_url, database_pool_max_size())


@contextmanager
def pooled_connection(database_url: str) -> Iterator[psycopg.Connection]:
    with get_connection_pool(database_url).connection() as connection:
        yield connection


def schema_state_table_exists(connection: psycopg.Connection) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", (_SCHEMA_STATE_TABLE,))
        row = cursor.fetchone()
    return bool(row and row[0])


def ensure_schema_state_table(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS ops")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ops.schema_state (
                component_name TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )


def read_schema_version(connection: psycopg.Connection, component_name: str) -> int | None:
    if not schema_state_table_exists(connection):
        return None

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT schema_version FROM ops.schema_state WHERE component_name = %s",
            (component_name,),
        )
        row = cursor.fetchone()
    return int(row[0]) if row is not None else None


def write_schema_version(
    connection: psycopg.Connection,
    *,
    component_name: str,
    schema_version: int,
) -> None:
    ensure_schema_state_table(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ops.schema_state (component_name, schema_version, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (component_name) DO UPDATE SET
                schema_version = EXCLUDED.schema_version,
                updated_at = NOW()
            """,
            (component_name, schema_version),
        )
