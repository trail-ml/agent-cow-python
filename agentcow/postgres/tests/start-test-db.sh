#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="agent-cow-pg"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"
PG_DBNAME="${PG_DBNAME:-agent_cow_test}"
PG_PORT="${PG_PORT:-5432}"
PG_VERSION="${PG_VERSION:-18}"

if docker inspect "$CONTAINER_NAME" &>/dev/null; then
    if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME")" = "true" ]; then
        echo "Container '$CONTAINER_NAME' is already running on port $PG_PORT."
    else
        echo "Starting existing container '$CONTAINER_NAME'..."
        docker start "$CONTAINER_NAME"
    fi
else
    echo "Creating container '$CONTAINER_NAME' (postgres:$PG_VERSION on port $PG_PORT)..."
    docker run --name "$CONTAINER_NAME" \
        -e POSTGRES_USER="$PG_USER" \
        -e POSTGRES_PASSWORD="$PG_PASSWORD" \
        -e POSTGRES_DB="$PG_DBNAME" \
        -p "$PG_PORT:5432" \
        -d "postgres:$PG_VERSION"
fi

echo "Waiting for PostgreSQL to be ready..."
until docker exec "$CONTAINER_NAME" pg_isready -U "$PG_USER" -q 2>/dev/null; do
    sleep 0.5
done

docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -tc \
    "SELECT 1 FROM pg_database WHERE datname = '$PG_DBNAME'" \
    | grep -q 1 \
    || docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -c "CREATE DATABASE \"$PG_DBNAME\""

echo "Seeding tables..."
docker exec -i "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DBNAME" <<'SQL'
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id serial PRIMARY KEY,
    owner_id integer NOT NULL REFERENCES users(id),
    title text NOT NULL,
    description text DEFAULT ''
);

CREATE TABLE IF NOT EXISTS tasks (
    id serial PRIMARY KEY,
    project_id integer NOT NULL REFERENCES projects(id),
    assigned_to integer REFERENCES users(id),
    title text NOT NULL,
    done boolean NOT NULL DEFAULT false
);

INSERT INTO users (name, email) VALUES
    ('Bessie', 'bessie@sunnymeadow.farm'),
    ('Clyde',  'clyde@lonepine.farm')
ON CONFLICT DO NOTHING;

INSERT INTO projects (owner_id, title, description) VALUES
    (1, 'North Pasture', 'Grazing rotation and fence maintenance'),
    (2, 'Dairy Barn',    'Milk production and storage')
ON CONFLICT DO NOTHING;

INSERT INTO tasks (project_id, assigned_to, title) VALUES
    (1, 1, 'Repair fencing'),
    (1, 2, 'Rotate hay bales'),
    (2, 1, 'Install milking equipment')
ON CONFLICT DO NOTHING;
SQL

echo "PostgreSQL is ready. To connect: docker exec -it "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DBNAME""
