from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.domain.identity import (
    AuthenticatedUser,
    normalize_email,
    normalize_role_collection,
)
from src.infrastructure.db import (
    DatabaseSchemaError,
    database_schema_bootstrap_enabled,
    ensure_schema_state_table,
    pooled_connection,
    read_schema_version,
    write_schema_version,
)

AUTH_SCHEMA_COMPONENT = "auth_access"
AUTH_SCHEMA_VERSION = 1

AUTH_SCHEMA_STATEMENTS: tuple[str, ...] = (
    "CREATE SCHEMA IF NOT EXISTS app",
    """
    CREATE TABLE IF NOT EXISTS app.event_log (
        event_id BIGSERIAL PRIMARY KEY,
        event_type TEXT NOT NULL,
        actor_type TEXT NOT NULL DEFAULT 'system',
        actor_id TEXT,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app.user_activity_log (
        activity_id BIGSERIAL PRIMARY KEY,
        user_id TEXT,
        session_id TEXT,
        event_name TEXT NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app.users (
        user_key TEXT PRIMARY KEY,
        email TEXT NOT NULL DEFAULT '',
        display_name TEXT NOT NULL DEFAULT '',
        tenant_id TEXT NOT NULL DEFAULT '',
        principal_id TEXT NOT NULL DEFAULT '',
        identity_provider TEXT NOT NULL DEFAULT '',
        auth_source TEXT NOT NULL DEFAULT '',
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app.user_role_assignments (
        subject_type TEXT NOT NULL,
        subject_value TEXT NOT NULL,
        role_name TEXT NOT NULL,
        assignment_source TEXT NOT NULL DEFAULT 'database',
        granted_by TEXT,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (subject_type, subject_value, role_name)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_log_type
    ON app.event_log (event_type, created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_user_activity_user
    ON app.user_activity_log (user_id, occurred_at DESC)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_user_activity_session_event
    ON app.user_activity_log (session_id, event_name)
    WHERE session_id IS NOT NULL
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_users_email
    ON app.users (email)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_user_role_assignments_subject
    ON app.user_role_assignments (subject_type, subject_value)
    WHERE is_active = TRUE
    """,
)


class UserAccessRepository:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection

    def _required_tables_present(self) -> bool:
        required_tables = (
            "app.event_log",
            "app.user_activity_log",
            "app.users",
            "app.user_role_assignments",
        )
        with self.connection.cursor() as cursor:
            for table_name in required_tables:
                cursor.execute("SELECT to_regclass(%s)", (table_name,))
                row = cursor.fetchone()
                if not row or not row[0]:
                    return False
        return True

    def bootstrap_database_objects(self) -> None:
        ensure_schema_state_table(self.connection)
        with self.connection.cursor() as cursor:
            for statement in AUTH_SCHEMA_STATEMENTS:
                cursor.execute(statement)
        write_schema_version(
            self.connection,
            component_name=AUTH_SCHEMA_COMPONENT,
            schema_version=AUTH_SCHEMA_VERSION,
        )

    def ensure_database_objects(self, *, allow_bootstrap: bool = False) -> None:
        current_version = read_schema_version(self.connection, AUTH_SCHEMA_COMPONENT)
        if current_version == AUTH_SCHEMA_VERSION:
            if not self._required_tables_present():
                raise DatabaseSchemaError(
                    "The authorization schema version is marked as current, "
                    "but required tables are missing. Reinitialize or migrate the database."
                )
            return

        if current_version is None and allow_bootstrap:
            self.bootstrap_database_objects()
            return

        if current_version is None:
            raise DatabaseSchemaError(
                "The authorization database schema is not initialized. "
                "Set DB_BOOTSTRAP_SCHEMA=1 for an explicit local bootstrap, "
                "or initialize the database before starting the app."
            )

        raise DatabaseSchemaError(
            "The authorization database schema version is incompatible. "
            f"Expected {AUTH_SCHEMA_VERSION}, found {current_version}. "
            "Apply the required database migration before running this service."
        )

    def upsert_user(self, user: AuthenticatedUser) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app.users (
                    user_key,
                    email,
                    display_name,
                    tenant_id,
                    principal_id,
                    identity_provider,
                    auth_source,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_key) DO UPDATE SET
                    email = EXCLUDED.email,
                    display_name = EXCLUDED.display_name,
                    tenant_id = EXCLUDED.tenant_id,
                    principal_id = EXCLUDED.principal_id,
                    identity_provider = EXCLUDED.identity_provider,
                    auth_source = EXCLUDED.auth_source,
                    updated_at = NOW(),
                    last_seen_at = NOW()
                """,
                (
                    user.user_key,
                    user.email,
                    user.display_name,
                    user.tenant_id,
                    user.principal_id,
                    user.identity_provider,
                    user.auth_source,
                ),
            )

    def load_roles(self, user: AuthenticatedUser) -> tuple[str, ...]:
        subject_groups = list(user.groups) or [""]
        normalized_email = normalize_email(user.email)

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT role_name
                FROM app.user_role_assignments
                WHERE is_active = TRUE
                  AND (
                      (subject_type = 'user_key' AND subject_value = %s)
                      OR (subject_type = 'email' AND subject_value = %s)
                      OR (subject_type = 'group' AND subject_value = ANY(%s))
                  )
                ORDER BY role_name
                """,
                (
                    user.user_key,
                    normalized_email,
                    subject_groups,
                ),
            )
            rows = cursor.fetchall()

        return normalize_role_collection(row["role_name"] for row in rows)

    def log_event(
        self,
        *,
        event_type: str,
        actor_type: str,
        actor_id: str | None,
        payload: dict[str, Any],
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app.event_log (event_type, actor_type, actor_id, payload)
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (
                    event_type,
                    actor_type,
                    actor_id,
                    json.dumps(payload),
                ),
            )

    def log_user_activity(
        self,
        *,
        user_id: str | None,
        session_id: str,
        event_name: str,
        payload: dict[str, Any],
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app.user_activity_log (user_id, session_id, event_name, payload)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (session_id, event_name) WHERE session_id IS NOT NULL DO NOTHING
                """,
                (
                    user_id,
                    session_id,
                    event_name,
                    json.dumps(payload),
                ),
            )


@contextmanager
def open_user_access_repository(
    database_url: str | None,
    *,
    ensure_objects: bool = True,
) -> Iterator[UserAccessRepository | None]:
    if not database_url:
        yield None
        return

    with pooled_connection(database_url) as connection:
        repository = UserAccessRepository(connection)
        if ensure_objects:
            repository.ensure_database_objects(
                allow_bootstrap=database_schema_bootstrap_enabled(),
            )
        yield repository
