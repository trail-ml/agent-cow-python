"""
Pure SQL operations for COW (Copy-On-Write) management.

This module provides database-agnostic SQL generation for COW operations.
All functions return executable SQL strings that work with any PostgreSQL-compatible
driver (SQLAlchemy, asyncpg, psycopg, PGlite, etc.).

No driver-specific imports are used — only standard Python + raw SQL.
"""

import uuid


COW_FUNCTION_NAMES = (
    "setup_cow",
    "commit_cow",
    "commit_cow_upsert",
    "commit_cow_delete",
    "commit_cow_cleanup",
    "discard_cow",
    "teardown_cow",
)


# ---------------------------------------------------------------------------
# SQL formatting helpers
# ---------------------------------------------------------------------------


def _quote_ident(s: str) -> str:
    """Quote a PostgreSQL identifier (table name, column name, schema, etc.)."""
    return '"' + s.replace('"', '""') + '"'


def _quote_literal(s: str) -> str:
    """Quote a PostgreSQL string literal, escaping single quotes."""
    return "'" + s.replace("'", "''") + "'"


def _validate_uuid(value: str | uuid.UUID) -> uuid.UUID:
    """Ensure *value* is a valid UUID, raising ``ValueError`` if not.

    The ``uuid.UUID`` constructor raises ``ValueError`` for invalid strings.
    """
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(value)


def _to_uuid(value: str | uuid.UUID) -> str:
    """Format a value as a PostgreSQL UUID literal."""
    return f"{_quote_literal(str(_validate_uuid(value)))}::uuid"


def _to_text_array(items: list[str]) -> str:
    """Format a list of strings as a PostgreSQL text[] literal."""
    if not items:
        return "ARRAY[]::text[]"
    vals = ",".join(_quote_literal(s) for s in items)
    return f"ARRAY[{vals}]::text[]"


def _to_uuid_array(uuids: list[str | uuid.UUID]) -> str:
    """Format a list of UUIDs as a PostgreSQL uuid[] literal."""
    if not uuids:
        return "ARRAY[]::uuid[]"
    vals = ",".join(_quote_literal(str(_validate_uuid(u))) for u in uuids)
    return f"ARRAY[{vals}]::uuid[]"


# ---------------------------------------------------------------------------
# Table setup / teardown
# ---------------------------------------------------------------------------


def setup_cow_sql(
    schema: str,
    base_table: str,
    view_name: str,
    pk_cols: list[str],
) -> str:
    """SQL to call the ``setup_cow`` PL/pgSQL function."""
    return (
        f"SELECT setup_cow("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(base_table)}, "
        f"{_quote_literal(view_name)}, "
        f"{_to_text_array(pk_cols)})"
    )


def teardown_cow_sql(schema: str, view_name: str) -> str:
    """SQL to call the ``teardown_cow`` PL/pgSQL function."""
    return (
        f"SELECT teardown_cow("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(view_name)})"
    )


def rename_table_sql(schema: str, from_name: str, to_name: str) -> str:
    """SQL to rename a table within a schema."""
    return (
        f"ALTER TABLE {_quote_ident(schema)}.{_quote_ident(from_name)} "
        f"RENAME TO {_quote_ident(to_name)}"
    )


# ---------------------------------------------------------------------------
# State introspection
# ---------------------------------------------------------------------------


def check_cow_state_sql(
    schema: str,
    original_table: str,
    base_table: str,
) -> str:
    """SQL to check existence of base table, original table, and COW view.

    Returns one row: ``(base_exists, original_is_table, view_exists)``.
    """
    ql = _quote_literal
    return (
        "SELECT "
        f"EXISTS(SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = {ql(schema)} AND table_name = {ql(base_table)} "
        f"AND table_type = 'BASE TABLE'), "
        f"EXISTS(SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = {ql(schema)} AND table_name = {ql(original_table)} "
        f"AND table_type = 'BASE TABLE'), "
        f"EXISTS(SELECT 1 FROM information_schema.views "
        f"WHERE table_schema = {ql(schema)} AND table_name = {ql(original_table)})"
    )


def check_cow_disable_state_sql(
    schema: str,
    original_table: str,
    base_table: str,
    changes_table: str,
) -> str:
    """SQL to check state before disabling COW.

    Returns one row: ``(base_exists, original_is_table, view_exists, changes_exists)``.
    """
    ql = _quote_literal
    return (
        "SELECT "
        f"EXISTS(SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = {ql(schema)} AND table_name = {ql(base_table)} "
        f"AND table_type = 'BASE TABLE'), "
        f"EXISTS(SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = {ql(schema)} AND table_name = {ql(original_table)} "
        f"AND table_type = 'BASE TABLE'), "
        f"EXISTS(SELECT 1 FROM information_schema.views "
        f"WHERE table_schema = {ql(schema)} AND table_name = {ql(original_table)}), "
        f"EXISTS(SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = {ql(schema)} AND table_name = {ql(changes_table)})"
    )


def check_table_is_base_table_sql(schema: str, table_name: str) -> str:
    """SQL to check if a table exists as a BASE TABLE."""
    return (
        "SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = {_quote_literal(schema)} "
        f"AND table_name = {_quote_literal(table_name)} "
        "AND table_type = 'BASE TABLE'"
    )


def get_table_pk_cols_sql(schema: str, table_name: str) -> str:
    """SQL to get the primary key column names for a table."""
    return (
        "SELECT kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "ON tc.constraint_name = kcu.constraint_name "
        "AND tc.table_schema = kcu.table_schema "
        "WHERE tc.constraint_type = 'PRIMARY KEY' "
        f"AND tc.table_schema = {_quote_literal(schema)} "
        f"AND tc.table_name = {_quote_literal(table_name)} "
        "ORDER BY kcu.ordinal_position"
    )


def check_cow_functions_deployed_sql() -> str:
    """SQL to check whether the core COW PL/pgSQL functions are deployed."""
    names = ", ".join(_quote_literal(n) for n in COW_FUNCTION_NAMES)
    return f"SELECT COUNT(*) FROM pg_proc WHERE proname IN ({names})"


def list_user_tables_sql(schema: str) -> str:
    """SQL to list all user tables eligible for COW (excludes ``_base`` and ``_changes`` tables)."""
    return (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = {_quote_literal(schema)} "
        "AND table_type = 'BASE TABLE' "
        "AND table_name NOT LIKE '%\\_base' "
        "AND table_name NOT LIKE '%\\_changes' "
        "AND table_name != 'cow_dirty_tables'"
    )


def list_base_tables_sql(schema: str) -> str:
    """SQL to list all ``*_base`` tables in a schema."""
    return (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = {_quote_literal(schema)} "
        "AND table_name LIKE '%_base' AND table_type = 'BASE TABLE'"
    )


def list_changes_tables_sql(schema: str) -> str:
    """SQL to list all ``*_changes`` tables in a schema."""
    return (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = {_quote_literal(schema)} "
        "AND table_name LIKE '%_changes'"
    )


def check_table_has_changes_sql(
    schema: str,
    changes_table: str,
    session_id: str | uuid.UUID,
    operation_ids: list[str | uuid.UUID] | None = None,
) -> str:
    """SQL to check if a changes table has rows for a session (and optionally specific operations)."""
    sql = (
        f"SELECT 1 FROM {_quote_ident(schema)}.{_quote_ident(changes_table)} "
        f"WHERE session_id = {_to_uuid(session_id)}"
    )
    if operation_ids:
        sql += f" AND operation_id = ANY({_to_uuid_array(operation_ids)})"
    return sql + " LIMIT 1"


# ---------------------------------------------------------------------------
# Session-level commit / discard
# ---------------------------------------------------------------------------


def commit_cow_session_sql(
    schema: str,
    base_table: str,
    pk_cols: list[str],
    session_id: str | uuid.UUID,
) -> str:
    """SQL to commit all COW changes for a session on one table."""
    return (
        f"SELECT commit_cow("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(base_table)}, "
        f"{_to_text_array(pk_cols)}, "
        f"{_to_uuid(session_id)})"
    )


def commit_cow_upsert_sql(
    schema: str,
    base_table: str,
    pk_cols: list[str],
    session_id: str | uuid.UUID,
    operation_ids: list[str | uuid.UUID] | None = None,
) -> str:
    """SQL to apply the upsert phase of a COW commit for one table.

    Commits non-deleted rows only, leaving deletes and cleanup to
    subsequent calls. Used by schema-level commits to phase inserts
    and deletes across tables in FK dependency order.
    """
    ops = (
        f"{_to_uuid_array(operation_ids)}"
        if operation_ids
        else "NULL::uuid[]"
    )
    return (
        f"SELECT commit_cow_upsert("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(base_table)}, "
        f"{_to_text_array(pk_cols)}, "
        f"{_to_uuid(session_id)}, "
        f"{ops})"
    )


def commit_cow_delete_sql(
    schema: str,
    base_table: str,
    pk_cols: list[str],
    session_id: str | uuid.UUID,
    operation_ids: list[str | uuid.UUID] | None = None,
) -> str:
    """SQL to apply the delete phase of a COW commit for one table."""
    ops = (
        f"{_to_uuid_array(operation_ids)}"
        if operation_ids
        else "NULL::uuid[]"
    )
    return (
        f"SELECT commit_cow_delete("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(base_table)}, "
        f"{_to_text_array(pk_cols)}, "
        f"{_to_uuid(session_id)}, "
        f"{ops})"
    )


def commit_cow_cleanup_sql(
    schema: str,
    base_table: str,
    session_id: str | uuid.UUID,
    operation_ids: list[str | uuid.UUID] | None = None,
) -> str:
    """SQL to clean up the changes table and dirty-tables entry after a commit."""
    ops = (
        f"{_to_uuid_array(operation_ids)}"
        if operation_ids
        else "NULL::uuid[]"
    )
    return (
        f"SELECT commit_cow_cleanup("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(base_table)}, "
        f"{_to_uuid(session_id)}, "
        f"{ops})"
    )


def get_cow_fk_edges_sql(schema: str, base_tables: list[str]) -> str:
    """SQL to fetch FK edges among a set of base tables.

    Returns rows of ``(parent_base_table, child_base_table, is_self_ref)``.
    """
    return (
        f"SELECT parent_base_table, child_base_table, is_self_ref "
        f"FROM _cow_fk_edges("
        f"{_quote_literal(schema)}, {_to_text_array(base_tables)})"
    )


def alter_fk_constraints_deferrable_sql(
    schema: str,
    base_table: str,
) -> str:
    """SQL to flip all non-deferrable FK constraints on *base_table* to
    ``DEFERRABLE INITIALLY IMMEDIATE``.

    Runs a DO block that discovers the constraints and alters each one.
    Idempotent: already-deferrable constraints are left alone.
    """
    return f"""
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class cls ON con.conrelid = cls.oid
        JOIN pg_namespace ns ON cls.relnamespace = ns.oid
        WHERE con.contype = 'f'
          AND ns.nspname = {_quote_literal(schema)}
          AND cls.relname = {_quote_literal(base_table)}
          AND NOT con.condeferrable
    LOOP
        EXECUTE format(
            'ALTER TABLE {_quote_ident(schema)}.{_quote_ident(base_table)} ALTER CONSTRAINT %I DEFERRABLE INITIALLY IMMEDIATE',
            r.conname
        );
    END LOOP;
END
$$;
""".strip()


def alter_fk_constraints_not_deferrable_sql(
    schema: str,
    base_table: str,
) -> str:
    """SQL to revert ``DEFERRABLE INITIALLY IMMEDIATE`` FKs on *base_table*
    back to ``NOT DEFERRABLE``.

    Only touches constraints currently marked ``DEFERRABLE INITIALLY IMMEDIATE``;
    constraints that are ``INITIALLY DEFERRED`` (explicitly chosen by the
    schema owner) are left alone.
    """
    return f"""
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class cls ON con.conrelid = cls.oid
        JOIN pg_namespace ns ON cls.relnamespace = ns.oid
        WHERE con.contype = 'f'
          AND ns.nspname = {_quote_literal(schema)}
          AND cls.relname = {_quote_literal(base_table)}
          AND con.condeferrable
          AND NOT con.condeferred
    LOOP
        EXECUTE format(
            'ALTER TABLE {_quote_ident(schema)}.{_quote_ident(base_table)} ALTER CONSTRAINT %I NOT DEFERRABLE',
            r.conname
        );
    END LOOP;
END
$$;
""".strip()


def discard_cow_session_sql(
    schema: str,
    base_table: str,
    session_id: str | uuid.UUID,
) -> str:
    """SQL to discard all COW changes for a session on one table."""
    return (
        f"SELECT discard_cow("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(base_table)}, "
        f"{_to_uuid(session_id)})"
    )


# ---------------------------------------------------------------------------
# Operation-level commit / discard
# ---------------------------------------------------------------------------


def commit_cow_operations_sql(
    schema: str,
    base_table: str,
    pk_cols: list[str],
    session_id: str | uuid.UUID,
    operation_ids: list[str | uuid.UUID],
) -> str:
    """SQL to commit specific operations from a COW session to the base table."""
    return (
        f"SELECT commit_cow("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(base_table)}, "
        f"{_to_text_array(pk_cols)}, "
        f"{_to_uuid(session_id)}, "
        f"{_to_uuid_array(operation_ids)})"
    )


def discard_cow_operations_sql(
    schema: str,
    base_table: str,
    session_id: str | uuid.UUID,
    operation_ids: list[str | uuid.UUID],
) -> str:
    """SQL to discard specific operations from a COW session."""
    return (
        f"SELECT discard_cow("
        f"{_quote_literal(schema)}, "
        f"{_quote_literal(base_table)}, "
        f"{_to_uuid(session_id)}, "
        f"{_to_uuid_array(operation_ids)})"
    )


# ---------------------------------------------------------------------------
# Querying session / operation metadata
# ---------------------------------------------------------------------------


def get_session_operations_sql(
    schema: str,
    session_id: str | uuid.UUID,
) -> str:
    """SQL to get all operation IDs in a COW session."""
    return (
        f"SELECT operation_id FROM get_cow_session_operations("
        f"{_quote_literal(schema)}, {_to_uuid(session_id)})"
    )


def get_operation_dependencies_sql(
    schema: str,
    session_id: str | uuid.UUID,
) -> str:
    """SQL to get dependency pairs (depends_on, operation_id) in a session."""
    return (
        f"SELECT depends_on, operation_id FROM get_cow_dependencies("
        f"{_quote_literal(schema)}, {_to_uuid(session_id)})"
    )


def set_visible_operations_sql(
    operation_ids: list[str | uuid.UUID] | None,
) -> str:
    """SQL to set which operations' changes are visible in subsequent queries."""
    if operation_ids:
        ops_str = ",".join(str(_validate_uuid(op)) for op in operation_ids)
        return f"SET LOCAL app.visible_operations = '{ops_str}'"
    return "SET LOCAL app.visible_operations = ''"


def get_dirty_tables_sql(
    schema: str,
    session_id: str | uuid.UUID,
) -> str:
    """SQL to get all dirty table names for a session from cow_dirty_tables."""
    return (
        "SELECT table_name FROM cow_dirty_tables "
        f"WHERE schema_name = {_quote_literal(schema)} "
        f"AND session_id = {_to_uuid(session_id)}"
    )
