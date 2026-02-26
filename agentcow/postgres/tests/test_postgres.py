"""Tests for agent-cow PostgreSQL COW implementation."""

import pytest

from agentcow.postgres import (
    Executor,
    CowRequestConfig,
    build_cow_variable_statements,
    deploy_cow_functions,
    enable_cow,
    disable_cow,
    enable_cow_schema,
    disable_cow_schema,
    apply_cow_variables,
    reset_cow_variables,
    is_cow_enabled,
    get_cow_status,
    commit_cow_session,
    discard_cow_session,
)


def test_executor_protocol_is_runtime_checkable():
    """Ensure the Executor protocol exposes an ``execute`` method so
    isinstance() checks work at runtime."""
    assert hasattr(Executor, "execute")


def test_cow_request_config_defaults():
    """A default CowRequestConfig should have COW disabled and all
    IDs set to None."""
    config = CowRequestConfig()
    assert config.agent_session_id is None
    assert config.operation_id is None
    assert config.visible_operations is None
    assert config.is_cow_requested is False


def test_cow_request_config_requested(session_id, operation_id):
    """Providing a session_id and operation_id should mark the config
    as COW-requested."""
    config = CowRequestConfig(
        agent_session_id=session_id,
        operation_id=operation_id,
    )
    assert config.is_cow_requested is True
    assert config.agent_session_id == session_id


def test_build_cow_variable_statements(session_id, operation_id):
    """With both IDs supplied, two SET statements should be produced
    (one for session_id, one for operation_id)."""
    stmts = build_cow_variable_statements(session_id, operation_id)
    assert len(stmts) == 2
    assert "app.session_id" in stmts[0]
    assert "app.operation_id" in stmts[1]


def test_build_cow_variable_statements_session_only(session_id):
    """When only a session_id is given, only one SET statement should
    be produced."""
    stmts = build_cow_variable_statements(session_id)
    assert len(stmts) == 1
    assert "app.session_id" in stmts[0]


# ---------------------------------------------------------------------------
# Integration tests (require Docker Postgres)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deploy_cow_functions(seeded_executor):
    """Deploying COW functions should register them in Postgres and
    be reflected in get_cow_status."""
    await deploy_cow_functions(seeded_executor)
    status = await get_cow_status(seeded_executor)
    assert status["cow_functions_deployed"] is True


@pytest.mark.asyncio
async def test_is_cow_enabled_no_session(seeded_executor):
    """is_cow_enabled returns False when no session ID is provided,
    even if the database is fully configured."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")
    config = CowRequestConfig()
    assert await is_cow_enabled(seeded_executor, config) is False


@pytest.mark.asyncio
async def test_is_cow_enabled_no_functions(seeded_executor, session_id):
    """is_cow_enabled returns False when the CoW functions haven't been
    deployed, even if a session ID is provided."""
    config = CowRequestConfig(agent_session_id=session_id)
    assert await is_cow_enabled(seeded_executor, config) is False


@pytest.mark.asyncio
async def test_is_cow_enabled_fully_configured(seeded_executor, session_id):
    """is_cow_enabled returns True only when both the request carries a
    session ID and the database has CoW functions + tables configured."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")
    config = CowRequestConfig(agent_session_id=session_id)
    assert await is_cow_enabled(seeded_executor, config) is True


@pytest.mark.asyncio
async def test_enable_cow_on_all_tables(seeded_executor):  # ✅
    """Enabling COW on multiple tables should list all of them in the
    status report."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")
    await enable_cow(seeded_executor, "projects")
    await enable_cow(seeded_executor, "tasks")

    status = await get_cow_status(seeded_executor)
    assert sorted(status["tables_with_cow"]) == ["projects", "tasks", "users"]


@pytest.mark.asyncio
async def test_cow_view_returns_base_data(seeded_executor):
    """Without an active session, the COW view should return the
    original base-table data unchanged."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert rows == [("Bessie",), ("Clyde",)]


@pytest.mark.asyncio
async def test_cow_insert_is_session_scoped(seeded_executor, session_id, operation_id):
    """An INSERT within a COW session should be intercepted into the changes
    table, visible through the view, and absent from the base table."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (name, email) VALUES ('Daisy', 'daisy@willowbrook.farm')"
    )

    view_rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert ("Daisy",) in view_rows

    base_rows = await seeded_executor.execute("SELECT name FROM users_base ORDER BY id")
    assert ("Daisy",) not in base_rows

    changes_rows = await seeded_executor.execute(
        "SELECT name FROM users_changes WHERE session_id = " f"'{session_id}'::uuid"
    )
    assert ("Daisy",) in changes_rows

    await reset_cow_variables(seeded_executor)

    view_rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert ("Daisy",) not in view_rows


@pytest.mark.asyncio
async def test_cow_delete_is_session_scoped(seeded_executor, session_id, operation_id):
    """A DELETE within a COW session should hide the row for that session
    but leave it visible once session variables are reset."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute("DELETE FROM users WHERE name = 'Bessie'")

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert ("Bessie",) not in rows

    await reset_cow_variables(seeded_executor)

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert ("Bessie",) in rows


@pytest.mark.asyncio
async def test_commit_cow_session(seeded_executor, session_id, operation_id):
    """Committing a COW session should merge its pending changes into
    the base table so they persist without session variables."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (name, email) VALUES ('Dolly', 'dolly@cloverfield.farm')"
    )

    await reset_cow_variables(seeded_executor)

    await commit_cow_session(seeded_executor, "users", session_id)

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert ("Dolly",) in rows


@pytest.mark.asyncio
async def test_discard_cow_session(seeded_executor, session_id, operation_id):
    """Discarding a COW session should drop all its pending changes,
    leaving the base table untouched."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (name, email) VALUES ('Rosie', 'rosie@redbarns.farm')"
    )

    await reset_cow_variables(seeded_executor)

    await discard_cow_session(seeded_executor, "users", session_id)

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert ("Rosie",) not in rows


@pytest.mark.asyncio
async def test_disable_cow_restores_table(seeded_executor):
    """Disabling COW on a table should remove it from the status report
    and restore direct access to the original table data."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")
    await disable_cow(seeded_executor, "users")

    status = await get_cow_status(seeded_executor)
    assert "users" not in status["tables_with_cow"]

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert rows == [("Bessie",), ("Clyde",)]


# ---------------------------------------------------------------------------
# Schema-level enable / disable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enable_cow_schema(seeded_executor):
    """enable_cow_schema should enable COW on every user table and return
    the list of newly enabled table names."""
    await deploy_cow_functions(seeded_executor)
    enabled = await enable_cow_schema(seeded_executor)

    assert sorted(enabled) == ["projects", "tasks", "users"]

    status = await get_cow_status(seeded_executor)
    assert sorted(status["tables_with_cow"]) == ["projects", "tasks", "users"]


@pytest.mark.asyncio
async def test_enable_cow_schema_skips_already_enabled(seeded_executor):
    """Tables that already have COW enabled should not be re-enabled."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    enabled = await enable_cow_schema(seeded_executor)
    assert "users" not in enabled
    assert sorted(enabled) == ["projects", "tasks"]


@pytest.mark.asyncio
async def test_enable_cow_schema_exclude(seeded_executor):
    """Tables listed in *exclude* should be skipped."""
    await deploy_cow_functions(seeded_executor)
    enabled = await enable_cow_schema(seeded_executor, exclude={"tasks"})

    assert "tasks" not in enabled
    assert sorted(enabled) == ["projects", "users"]


@pytest.mark.asyncio
async def test_disable_cow_schema(seeded_executor):
    """disable_cow_schema should tear down COW on all enabled tables and
    restore direct table access."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)
    disabled = await disable_cow_schema(seeded_executor)

    assert sorted(disabled) == ["projects", "tasks", "users"]

    status = await get_cow_status(seeded_executor)
    assert status["tables_with_cow"] == []

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert rows == [("Bessie",), ("Clyde",)]


@pytest.mark.asyncio
async def test_disable_cow_schema_exclude(seeded_executor):
    """Tables listed in *exclude* should remain COW-enabled after disable."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)
    disabled = await disable_cow_schema(seeded_executor, exclude={"users"})

    assert "users" not in disabled
    status = await get_cow_status(seeded_executor)
    assert "users" in status["tables_with_cow"]
