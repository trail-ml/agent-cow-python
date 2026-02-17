export const SETUP_COW_SQL = `
CREATE OR REPLACE FUNCTION setup_cow(
    p_base_table text,
    p_view_name  text,
    p_pk_col     text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name text;
    col_list           text;
    col_list_b         text;
    coalesce_list      text;
    changes_list       text;
    new_values_list    text;
    old_values_list    text;
    update_set_list    text;
    excluded_set_list  text;
BEGIN
    changes_table_name := p_view_name || '_changes';
    
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I (
           session_id text NOT NULL,
           operation_id text NOT NULL,
           LIKE %I INCLUDING DEFAULTS,
           _cow_deleted boolean NOT NULL DEFAULT false,
           _cow_updated_at timestamptz NOT NULL DEFAULT now(),
           PRIMARY KEY (session_id, operation_id, %I)
         )',
        changes_table_name,
        p_base_table,
        p_pk_col
    );
    
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (session_id, %I)',
        changes_table_name || '_session_pk_idx',
        changes_table_name,
        p_pk_col
    );

    SELECT
        string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position),
        string_agg(format('b.%I', column_name), ', ' ORDER BY ordinal_position),
        string_agg(format('COALESCE(c.%1$I, b.%1$I) AS %1$I', column_name), ', ' ORDER BY ordinal_position),
        string_agg(format('c.%I', column_name), ', ' ORDER BY ordinal_position),
        string_agg(format('NEW.%I', column_name), ', ' ORDER BY ordinal_position),
        string_agg(format('OLD.%I', column_name), ', ' ORDER BY ordinal_position),
        string_agg(
            CASE WHEN column_name != p_pk_col THEN
                format('%1$I = NEW.%1$I', column_name)
            END,
            ', ' ORDER BY ordinal_position
        ) FILTER (WHERE column_name != p_pk_col),
        string_agg(
            CASE WHEN column_name != p_pk_col THEN
                format('%1$I = EXCLUDED.%1$I', column_name)
            END,
            ', ' ORDER BY ordinal_position
        ) FILTER (WHERE column_name != p_pk_col)
    INTO
        col_list,
        col_list_b,
        coalesce_list,
        changes_list,
        new_values_list,
        old_values_list,
        update_set_list,
        excluded_set_list
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = p_base_table;

    EXECUTE format($v$
        CREATE OR REPLACE VIEW %I AS
        SELECT %s
        FROM %I b
        WHERE NULLIF(current_setting('app.session_id', true), '') IS NULL

        UNION ALL

        SELECT %s
        FROM %I b
        LEFT JOIN LATERAL (
            SELECT * FROM %I c2
            WHERE c2.session_id = NULLIF(current_setting('app.session_id', true), '')
              AND c2.%I = b.%I
              AND (
                    NULLIF(current_setting('app.visible_operations', true), '') IS NULL
                    OR c2.operation_id = ANY(string_to_array(current_setting('app.visible_operations', true), ','))
                  )
            ORDER BY c2._cow_updated_at DESC
            LIMIT 1
        ) c ON true
        WHERE NULLIF(current_setting('app.session_id', true), '') IS NOT NULL
          AND COALESCE(c._cow_deleted, false) = false

        UNION ALL

        SELECT %s
        FROM (
            SELECT DISTINCT ON (c3.%I) c3.*
            FROM %I c3
            WHERE c3.session_id = NULLIF(current_setting('app.session_id', true), '')
              AND (
                    NULLIF(current_setting('app.visible_operations', true), '') IS NULL
                    OR c3.operation_id = ANY(string_to_array(current_setting('app.visible_operations', true), ','))
                  )
            ORDER BY c3.%I, c3._cow_updated_at DESC
        ) c
        LEFT JOIN %I b ON b.%I = c.%I
        WHERE NULLIF(current_setting('app.session_id', true), '') IS NOT NULL
          AND b.%I IS NULL
          AND c._cow_deleted = false
    $v$,
        p_view_name,
        col_list_b,
        p_base_table,
        coalesce_list,
        p_base_table,
        changes_table_name,
        p_pk_col, p_pk_col,
        changes_list,
        p_pk_col,
        changes_table_name,
        p_pk_col,
        p_base_table, p_pk_col, p_pk_col,
        p_pk_col
    );

    IF update_set_list IS NOT NULL AND update_set_list != '' THEN
        EXECUTE format($f$
            CREATE OR REPLACE FUNCTION %I()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $trigger$
            DECLARE 
                sess text;
                op_id text;
            BEGIN
                sess := NULLIF(current_setting('app.session_id', true), '');
                
                IF sess IS NULL THEN
                    INSERT INTO %I (%s)
                    VALUES (%s)
                    ON CONFLICT (%I) DO UPDATE SET %s;
                ELSE
                    op_id := NULLIF(current_setting('app.operation_id', true), '');
                    IF op_id IS NULL THEN
                        RAISE EXCEPTION 'app.operation_id must be set when app.session_id is set';
                    END IF;
                    
                    INSERT INTO %I (session_id, operation_id, %s, _cow_deleted, _cow_updated_at)
                    VALUES (sess, op_id, %s, false, now())
                    ON CONFLICT (session_id, operation_id, %I) DO UPDATE
                        SET %s,
                            _cow_deleted = false,
                            _cow_updated_at = now();
                END IF;

                RETURN NEW;
            END;
            $trigger$
        $f$,
            p_view_name || '_cow_upsert',
            p_base_table,
            col_list,
            new_values_list,
            p_pk_col,
            update_set_list,
            changes_table_name,
            col_list,
            new_values_list,
            p_pk_col,
            excluded_set_list
        );
    ELSE
        EXECUTE format($f$
            CREATE OR REPLACE FUNCTION %I()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $trigger$
            DECLARE 
                sess text;
                op_id text;
            BEGIN
                sess := NULLIF(current_setting('app.session_id', true), '');
                
                IF sess IS NULL THEN
                    INSERT INTO %I (%s)
                    VALUES (%s)
                    ON CONFLICT (%I) DO NOTHING;
                ELSE
                    op_id := NULLIF(current_setting('app.operation_id', true), '');
                    IF op_id IS NULL THEN
                        RAISE EXCEPTION 'app.operation_id must be set when app.session_id is set';
                    END IF;
                    
                    INSERT INTO %I (session_id, operation_id, %s, _cow_deleted, _cow_updated_at)
                    VALUES (sess, op_id, %s, false, now())
                    ON CONFLICT (session_id, operation_id, %I) DO UPDATE
                        SET _cow_deleted = false,
                            _cow_updated_at = now();
                END IF;

                RETURN NEW;
            END;
            $trigger$
        $f$,
            p_view_name || '_cow_upsert',
            p_base_table,
            col_list,
            new_values_list,
            p_pk_col,
            changes_table_name,
            col_list,
            new_values_list,
            p_pk_col
        );
    END IF;

    EXECUTE format($f$
        CREATE OR REPLACE FUNCTION %I()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $trigger$
        DECLARE 
            sess text;
            op_id text;
        BEGIN
            sess := NULLIF(current_setting('app.session_id', true), '');
            
            IF sess IS NULL THEN
                DELETE FROM %I WHERE %I = OLD.%I;
            ELSE
                op_id := NULLIF(current_setting('app.operation_id', true), '');
                IF op_id IS NULL THEN
                    RAISE EXCEPTION 'app.operation_id must be set when app.session_id is set';
                END IF;
                
                INSERT INTO %I (session_id, operation_id, %s, _cow_deleted, _cow_updated_at)
                VALUES (sess, op_id, %s, true, now())
                ON CONFLICT (session_id, operation_id, %I) DO UPDATE
                    SET _cow_deleted = true,
                        _cow_updated_at = now();
            END IF;

            RETURN OLD;
        END;
        $trigger$
    $f$,
        p_view_name || '_cow_delete',
        p_base_table, p_pk_col, p_pk_col,
        changes_table_name,
        col_list,
        old_values_list,
        p_pk_col
    );

    EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %I',
        p_view_name || '_cow_upsert_trigger', p_view_name
    );
    EXECUTE format(
        'CREATE TRIGGER %I
           INSTEAD OF INSERT OR UPDATE ON %I
           FOR EACH ROW EXECUTE FUNCTION %I()',
        p_view_name || '_cow_upsert_trigger', p_view_name, p_view_name || '_cow_upsert'
    );

    EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %I',
        p_view_name || '_cow_delete_trigger', p_view_name
    );
    EXECUTE format(
        'CREATE TRIGGER %I
           INSTEAD OF DELETE ON %I
           FOR EACH ROW EXECUTE FUNCTION %I()',
        p_view_name || '_cow_delete_trigger', p_view_name, p_view_name || '_cow_delete'
    );
END;
$$;
`;

export const COMMIT_COW_OPERATIONS_SQL = `
CREATE OR REPLACE FUNCTION commit_cow_operations(
    p_base_table    text,
    p_pk_col        text,
    p_session_id    text,
    p_operation_ids text[]
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name text;
    col_list           text;
    update_set_clause  text;
BEGIN
    changes_table_name := regexp_replace(p_base_table, '_base$', '') || '_changes';
    
    SELECT 
        string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position),
        string_agg(
            CASE WHEN column_name != p_pk_col THEN
                format('%1$I = EXCLUDED.%1$I', column_name)
            END,
            ', ' ORDER BY ordinal_position
        ) FILTER (WHERE column_name != p_pk_col)
    INTO col_list, update_set_clause
    FROM information_schema.columns
    WHERE table_schema = 'public' 
      AND table_name = p_base_table;

    IF update_set_clause IS NOT NULL AND update_set_clause != '' THEN
        EXECUTE format($sql$
            INSERT INTO %I
            SELECT %s FROM (
                SELECT DISTINCT ON (%I) *
                FROM %I
                WHERE session_id = $1 
                  AND operation_id = ANY($2)
                  AND _cow_deleted = FALSE
                ORDER BY %I, _cow_updated_at DESC
            ) latest
            ON CONFLICT (%I) DO UPDATE SET %s
        $sql$, 
            p_base_table,
            col_list,
            p_pk_col,
            changes_table_name,
            p_pk_col,
            p_pk_col,
            update_set_clause
        )
        USING p_session_id, p_operation_ids;
    ELSE
        EXECUTE format($sql$
            INSERT INTO %I
            SELECT %s FROM (
                SELECT DISTINCT ON (%I) *
                FROM %I
                WHERE session_id = $1 
                  AND operation_id = ANY($2)
                  AND _cow_deleted = FALSE
                ORDER BY %I, _cow_updated_at DESC
            ) latest
            ON CONFLICT (%I) DO NOTHING
        $sql$, 
            p_base_table,
            col_list,
            p_pk_col,
            changes_table_name,
            p_pk_col,
            p_pk_col
        )
        USING p_session_id, p_operation_ids;
    END IF;

    EXECUTE format($sql$
        DELETE FROM %I b
        USING (
            SELECT DISTINCT ON (%I) *
            FROM %I
            WHERE session_id = $1 AND operation_id = ANY($2)
            ORDER BY %I, _cow_updated_at DESC
        ) c
        WHERE c._cow_deleted = TRUE
          AND b.%I = c.%I
    $sql$, p_base_table, p_pk_col, changes_table_name, p_pk_col, p_pk_col, p_pk_col)
    USING p_session_id, p_operation_ids;

    EXECUTE format('DELETE FROM %I WHERE session_id = $1 AND operation_id = ANY($2)', changes_table_name)
    USING p_session_id, p_operation_ids;
END;
$$;
`;

export const DISCARD_COW_OPERATIONS_SQL = `
CREATE OR REPLACE FUNCTION discard_cow_operations(
    p_base_table    text,
    p_session_id    text,
    p_operation_ids text[]
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changes_table_name text;
BEGIN
    changes_table_name := regexp_replace(p_base_table, '_base$', '') || '_changes';
    
    EXECUTE format('DELETE FROM %I WHERE session_id = $1 AND operation_id = ANY($2)', changes_table_name)
    USING p_session_id, p_operation_ids;
END;
$$;
`;

export const GET_COW_DEPENDENCIES_SQL = `
CREATE OR REPLACE FUNCTION get_cow_dependencies(p_session_id text)
RETURNS TABLE(depends_on text, operation_id text)
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
        WHERE t.table_schema = 'public'
          AND t.table_name LIKE '%_changes'
    LOOP
        SELECT array_agg(kcu.column_name ORDER BY kcu.ordinal_position) INTO pk_cols
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'public'
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
            FROM %I a
            JOIN %I b
              ON a.session_id = b.session_id
             AND %s
             AND a.operation_id != b.operation_id
            WHERE a.session_id = $1
              AND a._cow_updated_at < b._cow_updated_at
        $q$, tbl.table_name, tbl.table_name, pk_join_condition);
    END LOOP;

    FOR tbl IN
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = 'public'
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
              AND tc.table_schema = 'public'
              AND (tc.table_name = base_table_name || '_base' OR tc.table_name = base_table_name)
            GROUP BY kcu.column_name, ccu.table_name, ccu.column_name
        LOOP
            referenced_table_name := regexp_replace(fk.referenced_table, '_base$', '');
            referenced_changes_table := referenced_table_name || '_changes';

            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = referenced_changes_table
            ) THEN
                IF fk_query != '' THEN
                    fk_query := fk_query || ' UNION ';
                END IF;

                fk_query := fk_query || format($q$
                    SELECT a.operation_id as dep_on, b.operation_id as op_id
                    FROM (
                        SELECT operation_id, %I, MIN(_cow_updated_at) as earliest_change
                        FROM %I
                        WHERE session_id = $1 AND _cow_deleted = false
                        GROUP BY operation_id, %I
                    ) a
                    JOIN (
                        SELECT operation_id, %I, MIN(_cow_updated_at) as earliest_change
                        FROM %I
                        WHERE session_id = $1 AND _cow_deleted = false
                        GROUP BY operation_id, %I
                    ) b
                      ON a.%I = b.%I
                     AND a.operation_id != b.operation_id
                     AND a.earliest_change < b.earliest_change
                $q$,
                    fk.referenced_column,
                    referenced_changes_table,
                    fk.referenced_column,
                    fk.fk_column,
                    tbl.table_name,
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
`;
