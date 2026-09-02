#!/usr/bin/env bash
# Start the desk temporal knowledge service.
# API-compatible with ito-cloud-runtime/agent-fleet/graphiti but backed by
# SQLite for local use (no Docker/FalkorDB/OpenAI required).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

export GRAPHITI_DB="${GRAPHITI_DB:-$HOME/.hermes/profiles/ito/temporal_graph.db}"
export GRAPHITI_PORT="${GRAPHITI_PORT:-8098}"

exec /Users/affoon/.hermes/hermes-agent/venv/bin/python -m uvicorn desk_service:app \
  --host 127.0.0.1 \
  --port "$GRAPHITI_PORT" \
  --log-level warning
