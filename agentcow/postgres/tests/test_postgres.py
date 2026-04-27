"""Tests for agent-cow PostgreSQL COW implementation."""

import uuid

import pytest

from agentcow.postgres import (
    Executor,
    CowPostgresConfig,
    build_cow_variable_statements,
    deferred_fk_constraints,
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
    get_dirty_tables,
    commit_cow_session_schema,
    discard_cow_session_schema,
)


def test_executor_protocol_is_runtime_checkable():
    """Ensure the Executor protocol exposes an ``execute`` method so
    isinstance() checks work at runtime."""
    assert hasattr(Executor, "execute")


def test_cow_request_config_defaults():
    """A default CowPostgresConfig should have COW disabled and all
    IDs set to None."""
    config = CowPostgresConfig()
    assert config.session_id is None
    assert config.operation_id is None
    assert config.visible_operations is None
    assert config.is_active is False


def test_cow_request_config_requested(session_id, operation_id):
    """Providing a session_id and operation_id should mark the config
    as COW-requested."""
    config = CowPostgresConfig(
        session_id=session_id,
        operation_id=operation_id,
    )
    assert config.is_active is True
    assert config.session_id == session_id


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
    config = CowPostgresConfig()
    assert await is_cow_enabled(seeded_executor, config) is False


@pytest.mark.asyncio
async def test_is_cow_enabled_no_functions(seeded_executor, session_id):
    """is_cow_enabled returns False when the CoW functions haven't been
    deployed, even if a session ID is provided."""
    config = CowPostgresConfig(session_id=session_id)
    assert await is_cow_enabled(seeded_executor, config) is False


@pytest.mark.asyncio
async def test_is_cow_enabled_fully_configured(seeded_executor, session_id):
    """is_cow_enabled returns True only when both the request carries a
    session ID and the database has CoW functions + tables configured."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")
    config = CowPostgresConfig(session_id=session_id)
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


# ---------------------------------------------------------------------------
# Dirty tables tracking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dirty_tables_populated_on_insert(
    seeded_executor, session_id, operation_id
):
    """An INSERT within a COW session should record the table in cow_dirty_tables."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (name, email) VALUES ('Flora', 'flora@meadow.farm')"
    )

    dirty = await get_dirty_tables(seeded_executor, session_id)
    assert "users" in dirty

    await reset_cow_variables(seeded_executor)


@pytest.mark.asyncio
async def test_dirty_tables_populated_on_delete(
    seeded_executor, session_id, operation_id
):
    """A DELETE within a COW session should record the table in cow_dirty_tables."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute("DELETE FROM users WHERE name = 'Bessie'")

    dirty = await get_dirty_tables(seeded_executor, session_id)
    assert "users" in dirty

    await reset_cow_variables(seeded_executor)


@pytest.mark.asyncio
async def test_dirty_tables_cleaned_on_commit(
    seeded_executor, session_id, operation_id
):
    """Committing a session should remove the table from cow_dirty_tables
    when no changes remain."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (name, email) VALUES ('Hazel', 'hazel@orchard.farm')"
    )
    await reset_cow_variables(seeded_executor)

    dirty_before = await get_dirty_tables(seeded_executor, session_id)
    assert "users" in dirty_before

    await commit_cow_session(seeded_executor, "users", session_id)

    dirty_after = await get_dirty_tables(seeded_executor, session_id)
    assert "users" not in dirty_after


@pytest.mark.asyncio
async def test_dirty_tables_cleaned_on_discard(
    seeded_executor, session_id, operation_id
):
    """Discarding a session should remove the table from cow_dirty_tables
    when no changes remain."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "users")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (name, email) VALUES ('Maple', 'maple@grove.farm')"
    )
    await reset_cow_variables(seeded_executor)

    dirty_before = await get_dirty_tables(seeded_executor, session_id)
    assert "users" in dirty_before

    await discard_cow_session(seeded_executor, "users", session_id)

    dirty_after = await get_dirty_tables(seeded_executor, session_id)
    assert "users" not in dirty_after


@pytest.mark.asyncio
async def test_commit_cow_session_schema(seeded_executor, session_id, operation_id):
    """commit_cow_session_schema should commit all dirty tables for a session."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (name, email) VALUES ('Ivy', 'ivy@fern.farm')"
    )
    await seeded_executor.execute(
        "INSERT INTO projects (owner_id, title, description) VALUES (1, 'Garden', 'Herb garden')"
    )
    await reset_cow_variables(seeded_executor)

    dirty = await get_dirty_tables(seeded_executor, session_id)
    assert sorted(dirty) == ["projects", "users"]

    committed = await commit_cow_session_schema(seeded_executor, session_id)
    assert sorted(committed) == ["projects", "users"]

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert ("Ivy",) in rows

    dirty_after = await get_dirty_tables(seeded_executor, session_id)
    assert dirty_after == []


@pytest.mark.asyncio
async def test_discard_cow_session_schema(seeded_executor, session_id, operation_id):
    """discard_cow_session_schema should discard all dirty tables for a session."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (name, email) VALUES ('Olive', 'olive@vineyard.farm')"
    )
    await seeded_executor.execute(
        "INSERT INTO projects (owner_id, title, description) VALUES (1, 'Vineyard', 'Wine grapes')"
    )
    await reset_cow_variables(seeded_executor)

    dirty = await get_dirty_tables(seeded_executor, session_id)
    assert sorted(dirty) == ["projects", "users"]

    discarded = await discard_cow_session_schema(seeded_executor, session_id)
    assert sorted(discarded) == ["projects", "users"]

    rows = await seeded_executor.execute("SELECT name FROM users ORDER BY id")
    assert ("Olive",) not in rows

    dirty_after = await get_dirty_tables(seeded_executor, session_id)
    assert dirty_after == []


# ---------------------------------------------------------------------------
# FK-relationship commit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_schema_with_new_fk_referenced_rows(
    seeded_executor, session_id, operation_id
):
    """Committing a schema where a COW-inserted row references another
    COW-inserted row via FK should succeed.

    This is the scenario that was broken before deferred FK constraints:
    a new user is created in the session, then a project referencing that
    new user is also created.  If 'projects' is committed before 'users',
    the FK check would fail without deferral.
    """
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (id, name, email) VALUES (100, 'Petunia', 'petunia@hilltop.farm')"
    )
    await seeded_executor.execute(
        "INSERT INTO projects (owner_id, title, description) "
        "VALUES (100, 'Hilltop Garden', 'Herb garden on the hilltop')"
    )
    await reset_cow_variables(seeded_executor)

    committed = await commit_cow_session_schema(seeded_executor, session_id)
    assert sorted(committed) == ["projects", "users"]

    rows = await seeded_executor.execute(
        "SELECT name FROM users WHERE id = 100"
    )
    assert rows == [("Petunia",)]

    rows = await seeded_executor.execute(
        "SELECT title FROM projects WHERE owner_id = 100"
    )
    assert rows == [("Hilltop Garden",)]


@pytest.mark.asyncio
async def test_commit_schema_with_cascading_fk_across_three_tables(
    seeded_executor, session_id, operation_id
):
    """A full chain: new user -> new project (refs user) -> new task (refs
    project and user).  All created within a single COW session and committed
    via commit_cow_session_schema.  Without deferred constraints the commit
    would fail because tasks references both projects and users."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO users (id, name, email) "
        "VALUES (200, 'Clover', 'clover@greenacre.farm')"
    )
    await seeded_executor.execute(
        "INSERT INTO projects (id, owner_id, title, description) "
        "VALUES (200, 200, 'Green Acres', 'Organic vegetables')"
    )
    await seeded_executor.execute(
        "INSERT INTO tasks (project_id, assigned_to, title) "
        "VALUES (200, 200, 'Plant seeds')"
    )
    await reset_cow_variables(seeded_executor)

    dirty = await get_dirty_tables(seeded_executor, session_id)
    assert sorted(dirty) == ["projects", "tasks", "users"]

    committed = await commit_cow_session_schema(seeded_executor, session_id)
    assert sorted(committed) == ["projects", "tasks", "users"]

    rows = await seeded_executor.execute("SELECT name FROM users WHERE id = 200")
    assert rows == [("Clover",)]

    rows = await seeded_executor.execute("SELECT title FROM projects WHERE id = 200")
    assert rows == [("Green Acres",)]

    rows = await seeded_executor.execute(
        "SELECT title FROM tasks WHERE project_id = 200"
    )
    assert rows == [("Plant seeds",)]


@pytest.mark.asyncio
async def test_commit_single_table_with_fk_to_existing_row(
    seeded_executor, session_id, operation_id
):
    """Committing a single table where the FK target already exists in the
    base table should work without deferral (baseline sanity check)."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "projects")

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute(
        "INSERT INTO projects (owner_id, title, description) "
        "VALUES (1, 'Sunflower Field', 'Annual sunflower planting')"
    )
    await reset_cow_variables(seeded_executor)

    await commit_cow_session(seeded_executor, "projects", session_id)

    rows = await seeded_executor.execute(
        "SELECT title FROM projects WHERE title = 'Sunflower Field'"
    )
    assert rows == [("Sunflower Field",)]


@pytest.mark.asyncio
async def test_enable_cow_makes_fk_constraints_deferrable(seeded_executor):
    """After enable_cow, FK constraints on the base table should be marked
    as DEFERRABLE so that SET CONSTRAINTS ALL DEFERRED can work."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow(seeded_executor, "projects")

    rows = await seeded_executor.execute(
        "SELECT con.conname, con.condeferrable "
        "FROM pg_constraint con "
        "JOIN pg_class cls ON con.conrelid = cls.oid "
        "JOIN pg_namespace ns ON cls.relnamespace = ns.oid "
        "WHERE con.contype = 'f' "
        "  AND ns.nspname = 'public' "
        "  AND cls.relname = 'projects_base'"
    )
    assert len(rows) > 0, "projects_base should have at least one FK constraint"
    for conname, condeferrable in rows:
        assert condeferrable is True, (
            f"FK constraint {conname} on projects_base should be deferrable"
        )


@pytest.mark.asyncio
async def test_enable_cow_makes_multi_hop_fk_deferrable(seeded_executor):
    """tasks -> projects -> users: both levels of FK should become deferrable."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)

    for base_table in ("tasks_base", "projects_base"):
        rows = await seeded_executor.execute(
            "SELECT con.conname, con.condeferrable "
            "FROM pg_constraint con "
            "JOIN pg_class cls ON con.conrelid = cls.oid "
            "JOIN pg_namespace ns ON cls.relnamespace = ns.oid "
            "WHERE con.contype = 'f' "
            f"  AND ns.nspname = 'public' AND cls.relname = '{base_table}'"
        )
        for conname, condeferrable in rows:
            assert condeferrable is True, (
                f"FK constraint {conname} on {base_table} should be deferrable"
            )


@pytest.mark.asyncio
async def test_deferred_fk_constraints_context_manager(seeded_executor):
    """The deferred_fk_constraints context manager should defer and then
    re-enable FK constraint checks."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)

    async with deferred_fk_constraints(seeded_executor):
        await seeded_executor.execute(
            "INSERT INTO projects_base (id, owner_id, title, description) "
            "VALUES (999, 999, 'Phantom Project', 'References non-existent user')"
        )
        await seeded_executor.execute(
            "INSERT INTO users_base (id, name, email) "
            "VALUES (999, 'Ghost', 'ghost@phantom.farm')"
        )

    rows = await seeded_executor.execute(
        "SELECT title FROM projects_base WHERE id = 999"
    )
    assert rows == [("Phantom Project",)]
    rows = await seeded_executor.execute(
        "SELECT name FROM users_base WHERE id = 999"
    )
    assert rows == [("Ghost",)]


@pytest.mark.asyncio
async def test_commit_schema_delete_and_reinsert_with_fk(
    seeded_executor, session_id, operation_id
):
    """Deleting a user and reinserting with the same ID, while a project
    still references that user, should commit cleanly with deferral."""
    await deploy_cow_functions(seeded_executor)
    await enable_cow_schema(seeded_executor)

    await apply_cow_variables(seeded_executor, session_id, operation_id)
    await seeded_executor.execute("DELETE FROM users WHERE id = 1")
    await seeded_executor.execute(
        "INSERT INTO users (id, name, email) "
        "VALUES (1, 'Bessie Jr', 'bessiejr@sunnymeadow.farm')"
    )
    await reset_cow_variables(seeded_executor)

    committed = await commit_cow_session_schema(seeded_executor, session_id)
    assert "users" in committed

    rows = await seeded_executor.execute("SELECT name FROM users WHERE id = 1")
    assert rows == [("Bessie Jr",)]
