#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    echo "Usage: ./scripts/release.sh [patch|minor|major]"
    echo ""
    echo "Bumps the package version, commits, tags, and pushes to trigger PyPI publish."
    echo "Defaults to 'patch' if no argument is given."
    exit 0
fi

BUMP="${1:-patch}"

if ! command -v hatch &> /dev/null; then
    echo "hatch not found, installing via uv..."
    uv tool install hatch
fi

if [ -n "$(git status --porcelain)" ]; then
    echo "error: working directory is not clean, commit or stash changes first"
    exit 1
fi

hatch version "$BUMP"
VERSION=$(hatch version)

git add agentcow/__init__.py
git commit -m "bump to $VERSION"
git tag "v$VERSION"
git push && git push --tags

echo "released v$VERSION"
