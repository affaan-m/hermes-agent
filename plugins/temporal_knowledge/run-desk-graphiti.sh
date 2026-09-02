#!/usr/bin/env bash
# Start the desk temporal knowledge service (REAL graphiti-core + Kuzu).
# The OpenAI key is resolved from 1Password at startup via op — the
# OP_SERVICE_ACCOUNT_TOKEN comes from broker.env (mode 0600).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

set -a
. "$HOME/.hermes/profiles/ito/state/reauth-broker/broker.env"
set +a

export GRAPHITI_DB="${GRAPHITI_DB:-$HOME/.hermes/profiles/ito/graphiti_desk}"
export GRAPHITI_PORT="${GRAPHITI_PORT:-8098}"
export GRAPHITI_MODEL="${GRAPHITI_MODEL:-gpt-5.6-luna}"
export GRAPHITI_EMBED_MODEL="${GRAPHITI_EMBED_MODEL:-text-embedding-3-large}"

# Kuzu's native storage engine SIGSEGVs on concurrent writer transactions
# (NodeTable::initScanState under NodeTable::update; crash reports
# Python-2026-08-31-1716*/1726* and Python-2026-09-01-1619*/1630*). Multiple
# ingest workers crash-loop the service and silently drop the in-memory
# episode queue; the crashes also corrupted the store. Serialize ingestion,
# and default to the maintained Neo4j backend (loopback, brew services).
export GRAPHITI_INGEST_CONCURRENCY="${GRAPHITI_INGEST_CONCURRENCY:-1}"
export GRAPHITI_BACKEND="${GRAPHITI_BACKEND:-neo4j}"

exec /Users/affoon/.hermes/hermes-agent/venv/bin/python -m uvicorn desk_graphiti_service:app \
  --host 127.0.0.1 \
  --port "$GRAPHITI_PORT" \
  --log-level warning
