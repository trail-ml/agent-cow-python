import os
import uuid
from typing import Any

import psycopg
import pytest
from pytest_postgresql import factories

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_DBNAME = os.environ.get("PG_DBNAME", "agent_cow_test")


def _drop_test_db():
    """Force-drop the test database (and disconnect any lingering backends)."""
    with psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname="postgres",
        autocommit=True,
    ) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = '{PG_DBNAME}' AND pid <> pg_backend_pid()"
        )
        conn.execute(f'DROP DATABASE IF EXISTS "{PG_DBNAME}"')


@pytest.fixture(scope="session", autouse=True)
def _cleanup_stale_test_db():
    _drop_test_db()
    yield
    _drop_test_db()


postgresql_noproc = factories.postgresql_noproc(
    host=PG_HOST,
    port=PG_PORT,
    user=PG_USER,
    password=PG_PASSWORD,
    dbname=PG_DBNAME,
)
postgresql = factories.postgresql("postgresql_noproc")


class PsycopgExecutor:
    """Wraps a psycopg sync connection to satisfy the async Executor protocol.

    Does NOT auto-commit so that ``SET LOCAL`` session variables survive
    across multiple execute() calls within the same test (they are
    transaction-scoped in PostgreSQL).  Call :meth:`commit` explicitly
    after setup steps if you need changes visible from external sessions
    (e.g. a psql terminal while debugging).
    """

    def __init__(self, conn):
        self._conn = conn

    async def execute(self, sql: str) -> list[tuple[Any, ...]]:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            if cur.description:
                return [tuple(row) for row in cur.fetchall()]
            return []

    def commit(self):
        self._conn.commit()


SEED_SQL = """
CREATE TABLE users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text UNIQUE NOT NULL
);

CREATE TABLE projects (
    id serial PRIMARY KEY,
    owner_id integer NOT NULL REFERENCES users(id),
    title text NOT NULL,
    description text DEFAULT ''
);

CREATE TABLE tasks (
    id serial PRIMARY KEY,
    project_id integer NOT NULL REFERENCES projects(id),
    assigned_to integer REFERENCES users(id),
    title text NOT NULL,
    done boolean NOT NULL DEFAULT false
);

INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob',   'bob@example.com');

INSERT INTO projects (owner_id, title, description) VALUES
    (1, 'Alpha', 'First project'),
    (2, 'Beta',  'Second project');

INSERT INTO tasks (project_id, assigned_to, title) VALUES
    (1, 1, 'Design schema'),
    (1, 2, 'Write tests'),
    (2, 1, 'Set up CI');
"""


@pytest.fixture
def executor(postgresql):
    return PsycopgExecutor(postgresql)


@pytest.fixture
def seeded_executor(postgresql):
    """Executor with users, projects, and tasks tables already populated."""
    with postgresql.cursor() as cur:
        cur.execute(SEED_SQL)
        postgresql.commit()
    return PsycopgExecutor(postgresql)


@pytest.fixture
def session_id():
    return uuid.uuid4()


@pytest.fixture
def operation_id():
    return uuid.uuid4()
