"""
PostgreSQL SQL function definitions for COW (Copy-On-Write) functionality.

This module contains all the PL/pgSQL function definitions that are deployed
to the PostgreSQL database to enable COW operations.
"""

# =========================================================================================
# SQL Definitions - Unified Parameterized Version
# =========================================================================================

SETUP_COW_SQL = """
CREATE OR REPLACE FUNCTION setup_cow(
    p_schema     text,
    p_base_table text,    -- The actual table containing data (e.g., 'users_base')
    p_view_name  text,    -- The name for the COW view (e.g., 'users')
    p_pk_cols    text[]   -- Array of PK column names (supports single and composite PKs)
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name   text;
    
    qual_base            text := format('%I.%I', p_schema, p_base_table);
    qual_changes         text;
    qual_view            text := format('%I.%I', p_schema, p_view_name);

    col_list             text;
    col_list_prefixed_b  text;
    coalesce_select_list text;
    changes_select_list  text;
    excluded_set_list    text;
    new_values_list      text;
    old_values_list      text;
    base_update_set      text;
    
    -- Composite PK helpers
    pk_cols_quoted       text;  -- "col1", "col2"
    pk_join_condition    text;  -- c2."col1" = b."col1" AND c2."col2" = b."col2"
    pk_distinct_on       text;  -- c3."col1", c3."col2"
    pk_order_by          text;  -- c3."col1", c3."col2"
    pk_base_join         text;  -- b."col1" = c."col1" AND b."col2" = c."col2"
    pk_null_check        text;  -- b."col1" IS NULL
    pk_delete_condition  text;  -- "col1" = OLD."col1" AND "col2" = OLD."col2"
    pk_old_values        text;  -- OLD."col1", OLD."col2"

    upsert_fn_name       text;
    delete_fn_name       text;
    pk_col               text;
    base_table_owner     text;  -- Owner of the base table
BEGIN
    -- Derive changes table name from base table (strip _base suffix if present)
    IF p_base_table LIKE '%_base' THEN
        changes_table_name := regexp_replace(p_base_table, '_base$', '') || '_changes';
    ELSE
        changes_table_name := p_base_table || '_changes';
    END IF;
    
    qual_changes := format('%I.%I', p_schema, changes_table_name);
    
    -- Function names based on the view name
    upsert_fn_name := p_view_name || '_cow_upsert';
    delete_fn_name := p_view_name || '_cow_delete';
    
    -- Build composite PK helper strings
    pk_cols_quoted := (SELECT string_agg(quote_ident(col), ', ') FROM unnest(p_pk_cols) col);
    pk_join_condition := (SELECT string_agg(format('c2.%I = b.%I', col, col), ' AND ') FROM unnest(p_pk_cols) col);
    pk_distinct_on := (SELECT string_agg(format('c3.%I', col), ', ') FROM unnest(p_pk_cols) col);
    pk_order_by := pk_distinct_on;
    pk_base_join := (SELECT string_agg(format('b.%I = c.%I', col, col), ' AND ') FROM unnest(p_pk_cols) col);
    pk_null_check := format('b.%I IS NULL', p_pk_cols[1]);  -- Check first PK col for NULL
    pk_delete_condition := (SELECT string_agg(format('%I = OLD.%I', col, col), ' AND ') FROM unnest(p_pk_cols) col);
    pk_old_values := (SELECT string_agg(format('OLD.%I', col), ', ') FROM unnest(p_pk_cols) col);

    ----------------------------------------------------------------------
    -- 1. Create the changes table (if not exists)
    -- Includes operation_id for per-operation tracking within a session
    ----------------------------------------------------------------------
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %s (
           session_id uuid NOT NULL,
           operation_id uuid NOT NULL,
           LIKE %s INCLUDING DEFAULTS INCLUDING GENERATED,
           _cow_deleted boolean NOT NULL DEFAULT false,
           _cow_updated_at timestamptz NOT NULL DEFAULT now(),
           PRIMARY KEY (session_id, operation_id, %s)
         );',
        qual_changes,
        qual_base,
        pk_cols_quoted
    );
    
    -- Inherit ownership from base table
    SELECT tableowner INTO base_table_owner
    FROM pg_tables
    WHERE schemaname = p_schema AND tablename = p_base_table;
    
    IF base_table_owner IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s OWNER TO %I', qual_changes, base_table_owner);
    END IF;
    
    -- Create index for efficient operation lookups
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %s (session_id, %s)',
        changes_table_name || '_session_pk_idx',
        qual_changes,
        pk_cols_quoted
    );

    ----------------------------------------------------------------------
    -- 2. Build column lists from the base table
    ----------------------------------------------------------------------
    SELECT
        string_agg(quote_ident(c.column_name), ', ' ORDER BY c.ordinal_position),
        string_agg(
            format('b.%I', c.column_name),
            ', ' ORDER BY c.ordinal_position
        ),
        string_agg(
            format('COALESCE(c.%1$I, b.%1$I) AS %1$I', c.column_name),
            ', ' ORDER BY c.ordinal_position
        ),
        string_agg(
            format('c.%I', c.column_name),
            ', ' ORDER BY c.ordinal_position
        ),
        string_agg(
            CASE WHEN NOT (c.column_name = ANY(p_pk_cols)) THEN
                format('%1$I = COALESCE(EXCLUDED.%1$I, %2$s)', c.column_name, COALESCE(c.column_default, 'NULL'))
            END,
            ', ' ORDER BY c.ordinal_position
        ) FILTER (WHERE NOT (c.column_name = ANY(p_pk_cols))),
        string_agg(
            format('COALESCE(NEW.%I, %s)', c.column_name, COALESCE(c.column_default, 'NULL')),
            ', ' ORDER BY c.ordinal_position
        ),
        string_agg(
            format('OLD.%I', c.column_name),
            ', ' ORDER BY c.ordinal_position
        ),
        string_agg(
            CASE WHEN NOT (c.column_name = ANY(p_pk_cols)) THEN
                format('%1$I = COALESCE(NEW.%1$I, %2$s)', c.column_name, COALESCE(c.column_default, 'NULL'))
            END,
            ', ' ORDER BY c.ordinal_position
        ) FILTER (WHERE NOT (c.column_name = ANY(p_pk_cols)))
    INTO
        col_list,
        col_list_prefixed_b,
        coalesce_select_list,
        changes_select_list,
        excluded_set_list,
        new_values_list,
        old_values_list,
        base_update_set
    FROM information_schema.columns c
    WHERE c.table_schema = p_schema
      AND c.table_name   = p_base_table;

    ----------------------------------------------------------------------
    -- 3. Create the COW overlay view with conditional logic
    ----------------------------------------------------------------------
    EXECUTE format($v$
        CREATE OR REPLACE VIEW %s AS
        -- Branch 1: No COW session - fast path, return base table directly
        SELECT %s
        FROM %s b
        WHERE NULLIF(current_setting('app.session_id', true), '') IS NULL

        UNION ALL

        -- Branch 2: COW session active - base rows with changes overlay
        SELECT %s
        FROM %s b
        LEFT JOIN LATERAL (
            SELECT * FROM %s c2
            WHERE c2.session_id = NULLIF(current_setting('app.session_id', true), '')::uuid
              AND %s
              AND (
                    NULLIF(current_setting('app.visible_operations', true), '') IS NULL
                    OR c2.operation_id = ANY(
                         string_to_array(current_setting('app.visible_operations', true), ',')::uuid[]
                       )
                  )
            ORDER BY c2._cow_updated_at DESC
            LIMIT 1
        ) c ON true
        WHERE NULLIF(current_setting('app.session_id', true), '') IS NOT NULL
          AND COALESCE(c._cow_deleted, false) = false

        UNION ALL

        -- Branch 3: COW session active - new rows only in changes (not in base)
        SELECT %s
        FROM (
            SELECT DISTINCT ON (%s) c3.*
            FROM %s c3
            WHERE c3.session_id = NULLIF(current_setting('app.session_id', true), '')::uuid
              AND (
                    NULLIF(current_setting('app.visible_operations', true), '') IS NULL
                    OR c3.operation_id = ANY(
                         string_to_array(current_setting('app.visible_operations', true), ',')::uuid[]
                       )
                  )
            ORDER BY %s, c3._cow_updated_at DESC
        ) c
        LEFT JOIN %s b ON %s
        WHERE NULLIF(current_setting('app.session_id', true), '') IS NOT NULL
          AND %s
          AND c._cow_deleted = false;
    $v$,
        qual_view,
        col_list_prefixed_b,
        qual_base,
        coalesce_select_list,
        qual_base,
        qual_changes,
        pk_join_condition,
        changes_select_list,
        pk_distinct_on,
        qual_changes,
        pk_order_by,
        qual_base,
        pk_base_join,
        pk_null_check
    );

    -- Inherit ownership from base table for the view
    IF base_table_owner IS NOT NULL THEN
        EXECUTE format('ALTER VIEW %s OWNER TO %I', qual_view, base_table_owner);
    END IF;

    ----------------------------------------------------------------------
    -- 4. Upsert trigger function
    ----------------------------------------------------------------------
    IF base_update_set IS NULL OR base_update_set = '' THEN
        -- Pure association table: all columns are PK, no update needed
        EXECUTE format($f$
            CREATE OR REPLACE FUNCTION %I.%I()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $trigger$
            DECLARE 
                sess uuid;
                sess_str text;
                op_id uuid;
                op_str text;
            BEGIN
                sess_str := NULLIF(current_setting('app.session_id', true), '');
                IF sess_str IS NOT NULL THEN
                    sess := sess_str::uuid;
                END IF;
                
                IF sess IS NULL THEN
                    INSERT INTO %s (%s)
                    VALUES (%s)
                    ON CONFLICT (%s) DO NOTHING;
                ELSE
                    op_str := NULLIF(current_setting('app.operation_id', true), '');
                    IF op_str IS NULL THEN
                        RAISE EXCEPTION 'app.operation_id must be set when app.session_id is set';
                    END IF;
                    op_id := op_str::uuid;
                    
                    INSERT INTO %s (session_id, operation_id, %s, _cow_deleted, _cow_updated_at)
                    VALUES (sess, op_id, %s, false, now())
                    ON CONFLICT (session_id, operation_id, %s) DO UPDATE
                        SET _cow_deleted = false,
                            _cow_updated_at = now();
                END IF;

                RETURN NEW;
            END;
            $trigger$;
        $f$,
            p_schema, upsert_fn_name,
            qual_base,
            col_list,
            new_values_list,
            pk_cols_quoted,
            qual_changes,
            col_list,
            new_values_list,
            pk_cols_quoted
        );
    ELSE
        -- Regular table with non-PK columns
        EXECUTE format($f$
            CREATE OR REPLACE FUNCTION %I.%I()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $trigger$
            DECLARE 
                sess uuid;
                sess_str text;
                op_id uuid;
                op_str text;
            BEGIN
                sess_str := NULLIF(current_setting('app.session_id', true), '');
                IF sess_str IS NOT NULL THEN
                    sess := sess_str::uuid;
                END IF;
                
                IF sess IS NULL THEN
                    INSERT INTO %s (%s)
                    VALUES (%s)
                    ON CONFLICT (%s) DO UPDATE SET %s;
                ELSE
                    op_str := NULLIF(current_setting('app.operation_id', true), '');
                    IF op_str IS NULL THEN
                        RAISE EXCEPTION 'app.operation_id must be set when app.session_id is set';
                    END IF;
                    op_id := op_str::uuid;
                    
                    INSERT INTO %s (session_id, operation_id, %s, _cow_deleted, _cow_updated_at)
                    VALUES (sess, op_id, %s, false, now())
                    ON CONFLICT (session_id, operation_id, %s) DO UPDATE
                        SET %s,
                            _cow_deleted = false,
                            _cow_updated_at = now();
                END IF;

                RETURN NEW;
            END;
            $trigger$;
        $f$,
            p_schema, upsert_fn_name,
            qual_base,
            col_list,
            new_values_list,
            pk_cols_quoted,
            base_update_set,
            qual_changes,
            col_list,
            new_values_list,
            pk_cols_quoted,
            excluded_set_list
        );
    END IF;

    ----------------------------------------------------------------------
    -- 5. Delete trigger function
    ----------------------------------------------------------------------
    EXECUTE format($f$
        CREATE OR REPLACE FUNCTION %I.%I()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $trigger$
        DECLARE 
            sess uuid;
            sess_str text;
            op_id uuid;
            op_str text;
        BEGIN
            sess_str := NULLIF(current_setting('app.session_id', true), '');
            IF sess_str IS NOT NULL THEN
                sess := sess_str::uuid;
            END IF;
            
            IF sess IS NULL THEN
                DELETE FROM %s WHERE %s;
            ELSE
                op_str := NULLIF(current_setting('app.operation_id', true), '');
                IF op_str IS NULL THEN
                    RAISE EXCEPTION 'app.operation_id must be set when app.session_id is set';
                END IF;
                op_id := op_str::uuid;
                
                INSERT INTO %s (session_id, operation_id, %s, _cow_deleted, _cow_updated_at)
                VALUES (sess, op_id, %s, true, now())
                ON CONFLICT (session_id, operation_id, %s) DO UPDATE
                    SET _cow_deleted = true,
                        _cow_updated_at = now();
            END IF;

            RETURN OLD;
        END;
        $trigger$;
    $f$,
        p_schema, delete_fn_name,
        qual_base, pk_delete_condition,
        qual_changes,
        col_list,
        old_values_list,
        pk_cols_quoted
    );

    ----------------------------------------------------------------------
    -- 6. Attach triggers to the COW view
    ----------------------------------------------------------------------
    EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %s;',
        upsert_fn_name || '_trigger', qual_view
    );
    EXECUTE format(
        'CREATE TRIGGER %I
           INSTEAD OF INSERT OR UPDATE ON %s
           FOR EACH ROW EXECUTE FUNCTION %I.%I();',
        upsert_fn_name || '_trigger', qual_view, p_schema, upsert_fn_name
    );

    EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %s;',
        delete_fn_name || '_trigger', qual_view
    );
    EXECUTE format(
        'CREATE TRIGGER %I
           INSTEAD OF DELETE ON %s
           FOR EACH ROW EXECUTE FUNCTION %I.%I();',
        delete_fn_name || '_trigger', qual_view, p_schema, delete_fn_name
    );

END;
$$;
"""

COMMIT_COW_SQL = """
CREATE OR REPLACE FUNCTION commit_cow(
    p_schema     text,
    p_base_table text,
    p_pk_cols    text[],
    p_session    uuid
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name text;
    qual_base          text := format('%I.%I', p_schema, p_base_table);
    qual_changes       text;
    pk_cols_quoted     text;
    pk_join_condition  text;
    update_set_clause  text;
BEGIN
    IF p_base_table LIKE '%_base' THEN
        changes_table_name := regexp_replace(p_base_table, '_base$', '') || '_changes';
    ELSE
        changes_table_name := p_base_table || '_changes';
    END IF;
    qual_changes := format('%I.%I', p_schema, changes_table_name);
    
    pk_cols_quoted := (SELECT string_agg(quote_ident(col), ', ') FROM unnest(p_pk_cols) col);
    pk_join_condition := (SELECT string_agg(format('c.%I = b.%I', col, col), ' AND ') FROM unnest(p_pk_cols) col);
    
    SELECT string_agg(
        format('%1$I = EXCLUDED.%1$I', column_name),
        ', ' ORDER BY ordinal_position
    )
    INTO update_set_clause
    FROM information_schema.columns
    WHERE table_schema = p_schema 
      AND table_name = p_base_table
      AND NOT (column_name = ANY(p_pk_cols));

    IF update_set_clause IS NULL OR update_set_clause = '' THEN
        EXECUTE format($sql$
            INSERT INTO %s
            SELECT %s FROM (
                SELECT DISTINCT ON (%s) *
                FROM %s
                WHERE session_id = $1 AND _cow_deleted = FALSE
                ORDER BY %s, _cow_updated_at DESC
            ) latest
            ON CONFLICT (%s) DO NOTHING
        $sql$, 
            qual_base,
            (SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
             FROM information_schema.columns
             WHERE table_schema = p_schema AND table_name = p_base_table),
            pk_cols_quoted,
            qual_changes,
            pk_cols_quoted,
            pk_cols_quoted
        )
        USING p_session;
    ELSE
        EXECUTE format($sql$
            INSERT INTO %s
            SELECT %s FROM (
                SELECT DISTINCT ON (%s) *
                FROM %s
                WHERE session_id = $1 AND _cow_deleted = FALSE
                ORDER BY %s, _cow_updated_at DESC
            ) latest
            ON CONFLICT (%s) DO UPDATE
                SET %s
        $sql$, 
            qual_base,
            (SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
             FROM information_schema.columns
             WHERE table_schema = p_schema AND table_name = p_base_table),
            pk_cols_quoted,
            qual_changes,
            pk_cols_quoted,
            pk_cols_quoted,
            update_set_clause
        )
        USING p_session;
    END IF;

    EXECUTE format($sql$
        DELETE FROM %s b
        USING (
            SELECT DISTINCT ON (%s) *
            FROM %s
            WHERE session_id = $1
            ORDER BY %s, _cow_updated_at DESC
        ) c
        WHERE c._cow_deleted = TRUE
          AND %s
    $sql$, qual_base, pk_cols_quoted, qual_changes, pk_cols_quoted, pk_join_condition)
    USING p_session;

    EXECUTE format('DELETE FROM %s WHERE session_id = $1', qual_changes)
    USING p_session;
END;
$$;
"""

DISCARD_COW_SQL = """
CREATE OR REPLACE FUNCTION discard_cow(
    p_schema     text,
    p_base_table text,
    p_session    uuid
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name text;
    qual_changes       text;
BEGIN
    IF p_base_table LIKE '%_base' THEN
        changes_table_name := regexp_replace(p_base_table, '_base$', '') || '_changes';
    ELSE
        changes_table_name := p_base_table || '_changes';
    END IF;
    qual_changes := format('%I.%I', p_schema, changes_table_name);
    
    EXECUTE format('DELETE FROM %s WHERE session_id = $1', qual_changes)
    USING p_session;
END;
$$;
"""

TEARDOWN_COW_SQL = """
CREATE OR REPLACE FUNCTION teardown_cow(
    p_schema    text,
    p_view_name text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name text;
    upsert_fn_name     text;
    delete_fn_name     text;
BEGIN
    changes_table_name := p_view_name || '_changes';
    upsert_fn_name := p_view_name || '_cow_upsert';
    delete_fn_name := p_view_name || '_cow_delete';

    EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', p_schema, p_view_name);
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', p_schema, changes_table_name);
    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I()', p_schema, upsert_fn_name);
    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I()', p_schema, delete_fn_name);
END;
$$;
"""

# =========================================================================================
# SQL Functions for Operation-Level COW Management
# =========================================================================================

GET_COW_DEPENDENCIES_SQL = """
CREATE OR REPLACE FUNCTION get_cow_dependencies(
    p_schema     text,
    p_session_id uuid
)
RETURNS TABLE(depends_on uuid, operation_id uuid)
LANGUAGE plpgsql
AS $$
DECLARE
    tbl RECORD;
    fk RECORD;
    query text := '';
    fk_query text := '';
    pk_cols text[];
    pk_join_condition text;
    base_table_name text;
    referenced_table_name text;
    referenced_changes_table text;
BEGIN
    FOR tbl IN 
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = p_schema 
          AND t.table_name LIKE '%_changes'
    LOOP
        SELECT array_agg(kcu.column_name ORDER BY kcu.ordinal_position) INTO pk_cols
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = p_schema
          AND tc.table_name = tbl.table_name
          AND kcu.ordinal_position >= 3;
        
        IF pk_cols IS NULL OR array_length(pk_cols, 1) IS NULL THEN
            CONTINUE;
        END IF;
        
        pk_join_condition := (SELECT string_agg(format('a.%I = b.%I', col, col), ' AND ') FROM unnest(pk_cols) col);
        
        IF query != '' THEN
            query := query || ' UNION ';
        END IF;
        
        query := query || format($q$
            SELECT DISTINCT a.operation_id as dep_on, b.operation_id as op_id
            FROM %I.%I a
            JOIN %I.%I b 
              ON a.session_id = b.session_id
             AND %s
             AND a.operation_id != b.operation_id
            WHERE a.session_id = $1
              AND a._cow_updated_at < b._cow_updated_at
        $q$, p_schema, tbl.table_name, p_schema, tbl.table_name, pk_join_condition);
    END LOOP;
    
    FOR tbl IN 
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = p_schema 
          AND t.table_name LIKE '%_changes'
    LOOP
        base_table_name := regexp_replace(tbl.table_name, '_changes$', '');
        
        FOR fk IN
            SELECT
                kcu.column_name AS fk_column,
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = p_schema
              AND (tc.table_name = base_table_name || '_base' OR tc.table_name = base_table_name)
            GROUP BY kcu.column_name, ccu.table_name, ccu.column_name
        LOOP
            referenced_table_name := regexp_replace(fk.referenced_table, '_base$', '');
            referenced_changes_table := referenced_table_name || '_changes';
            
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = p_schema
                  AND table_name = referenced_changes_table
            ) THEN
                IF fk_query != '' THEN
                    fk_query := fk_query || ' UNION ';
                END IF;
                
                fk_query := fk_query || format($q$
                    SELECT a.operation_id as dep_on, b.operation_id as op_id
                    FROM (
                        SELECT operation_id, %I, MIN(_cow_updated_at) as earliest_change
                        FROM %I.%I
                        WHERE session_id = $1 AND _cow_deleted = false
                        GROUP BY operation_id, %I
                    ) a
                    JOIN (
                        SELECT operation_id, %I, MIN(_cow_updated_at) as earliest_change
                        FROM %I.%I
                        WHERE session_id = $1 AND _cow_deleted = false
                        GROUP BY operation_id, %I
                    ) b
                      ON a.%I = b.%I
                     AND a.operation_id != b.operation_id
                     AND a.earliest_change < b.earliest_change
                $q$, 
                    fk.referenced_column,
                    p_schema, referenced_changes_table,
                    fk.referenced_column,
                    fk.fk_column,
                    p_schema, tbl.table_name,
                    fk.fk_column,
                    fk.referenced_column, fk.fk_column
                );
            END IF;
        END LOOP;
    END LOOP;
    
    IF query = '' AND fk_query = '' THEN
        RETURN;
    END IF;
    
    IF query != '' AND fk_query != '' THEN
        query := query || ' UNION ' || fk_query;
    ELSIF fk_query != '' THEN
        query := fk_query;
    END IF;
    
    RETURN QUERY EXECUTE query USING p_session_id;
END;
$$;
"""

COMMIT_COW_OPERATIONS_SQL = """
CREATE OR REPLACE FUNCTION commit_cow_operations(
    p_schema        text,
    p_base_table    text,
    p_pk_cols       text[],
    p_session_id    uuid,
    p_operation_ids uuid[]
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name text;
    qual_base          text := format('%I.%I', p_schema, p_base_table);
    qual_changes       text;
    pk_cols_quoted     text;
    pk_join_condition  text;
    update_set_clause  text;
BEGIN
    IF p_base_table LIKE '%_base' THEN
        changes_table_name := regexp_replace(p_base_table, '_base$', '') || '_changes';
    ELSE
        changes_table_name := p_base_table || '_changes';
    END IF;
    qual_changes := format('%I.%I', p_schema, changes_table_name);
    
    pk_cols_quoted := (SELECT string_agg(quote_ident(col), ', ') FROM unnest(p_pk_cols) col);
    pk_join_condition := (SELECT string_agg(format('c.%I = b.%I', col, col), ' AND ') FROM unnest(p_pk_cols) col);
    
    SELECT string_agg(
        format('%1$I = EXCLUDED.%1$I', column_name),
        ', ' ORDER BY ordinal_position
    )
    INTO update_set_clause
    FROM information_schema.columns
    WHERE table_schema = p_schema 
      AND table_name = p_base_table
      AND NOT (column_name = ANY(p_pk_cols));

    IF update_set_clause IS NULL OR update_set_clause = '' THEN
        EXECUTE format($sql$
            INSERT INTO %s
            SELECT %s FROM (
                SELECT DISTINCT ON (%s) *
                FROM %s
                WHERE session_id = $1 
                  AND operation_id = ANY($2)
                  AND _cow_deleted = FALSE
                ORDER BY %s, _cow_updated_at DESC
            ) latest
            ON CONFLICT (%s) DO NOTHING
        $sql$, 
            qual_base,
            (SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
             FROM information_schema.columns
             WHERE table_schema = p_schema AND table_name = p_base_table),
            pk_cols_quoted,
            qual_changes,
            pk_cols_quoted,
            pk_cols_quoted
        )
        USING p_session_id, p_operation_ids;
    ELSE
        EXECUTE format($sql$
            INSERT INTO %s
            SELECT %s FROM (
                SELECT DISTINCT ON (%s) *
                FROM %s
                WHERE session_id = $1 
                  AND operation_id = ANY($2)
                  AND _cow_deleted = FALSE
                ORDER BY %s, _cow_updated_at DESC
            ) latest
            ON CONFLICT (%s) DO UPDATE
                SET %s
        $sql$, 
            qual_base,
            (SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
             FROM information_schema.columns
             WHERE table_schema = p_schema AND table_name = p_base_table),
            pk_cols_quoted,
            qual_changes,
            pk_cols_quoted,
            pk_cols_quoted,
            update_set_clause
        )
        USING p_session_id, p_operation_ids;
    END IF;

    EXECUTE format($sql$
        DELETE FROM %s b
        USING (
            SELECT DISTINCT ON (%s) *
            FROM %s
            WHERE session_id = $1 AND operation_id = ANY($2)
            ORDER BY %s, _cow_updated_at DESC
        ) c
        WHERE c._cow_deleted = TRUE
          AND %s
    $sql$, qual_base, pk_cols_quoted, qual_changes, pk_cols_quoted, pk_join_condition)
    USING p_session_id, p_operation_ids;

    EXECUTE format('DELETE FROM %s WHERE session_id = $1 AND operation_id = ANY($2)', qual_changes)
    USING p_session_id, p_operation_ids;
END;
$$;
"""

DISCARD_COW_OPERATIONS_SQL = """
CREATE OR REPLACE FUNCTION discard_cow_operations(
    p_schema        text,
    p_base_table    text,
    p_session_id    uuid,
    p_operation_ids uuid[]
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name text;
    qual_changes       text;
BEGIN
    IF p_base_table LIKE '%_base' THEN
        changes_table_name := regexp_replace(p_base_table, '_base$', '') || '_changes';
    ELSE
        changes_table_name := p_base_table || '_changes';
    END IF;
    qual_changes := format('%I.%I', p_schema, changes_table_name);
    
    EXECUTE format('DELETE FROM %s WHERE session_id = $1 AND operation_id = ANY($2)', qual_changes)
    USING p_session_id, p_operation_ids;
END;
$$;
"""

GET_SESSION_OPERATIONS_SQL = """
CREATE OR REPLACE FUNCTION get_cow_session_operations(
    p_schema     text,
    p_session_id uuid
)
RETURNS TABLE(operation_id uuid, earliest_change timestamptz)
LANGUAGE plpgsql
AS $$
DECLARE
    tbl RECORD;
    query text := '';
BEGIN
    FOR tbl IN 
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = p_schema 
          AND t.table_name LIKE '%_changes'
    LOOP
        IF query != '' THEN
            query := query || ' UNION ALL ';
        END IF;
        
        query := query || format($q$
            SELECT operation_id, MIN(_cow_updated_at) as earliest_change
            FROM %I.%I
            WHERE session_id = $1
            GROUP BY operation_id
        $q$, p_schema, tbl.table_name);
    END LOOP;
    
    IF query = '' THEN
        RETURN;
    END IF;
    
    RETURN QUERY EXECUTE format($q$
        SELECT operation_id, MIN(earliest_change) as earliest_change
        FROM (%s) combined
        GROUP BY operation_id
        ORDER BY earliest_change
    $q$, query) USING p_session_id;
END;
$$;
"""
