# Temporal Knowledge Graph — Desk Verification Layer

A bi-temporal fact store for the Itô ledger that prevents stale-baseline drafting errors (e.g., approval 267 asking about specs after a contract was already signed).

## Architecture

This layer reuses the **ito-cloud-runtime/agent-fleet/graphiti** pattern but adapted for local desk use:

| Upstream (fleet) | Desk (local) |
|------------------|--------------|
| FalkorDB (Docker) | SQLite (embedded) |
| OpenAI LLM extraction | Deterministic structured ingestion |
| FastAPI service on :8099 | FastAPI service on :8098 |
| group_id silo per agent | group_id = "desk" |
| `/episode`, `/search`, `/brief` | Same endpoints + `/baseline`, `/ingest/ledger` |

The API shape is identical so migrating to real Graphiti is a config change when credentials are available.

### Why not Graphiti + Neo4j/FalkorDB locally?

- No Docker on this machine
- No OpenAI credits / funded Nous account for LLM extraction
- The ledger is already structured — deterministic ingestion is faster and more accurate than LLM parsing

Graphiti remains the migration target. The SQLite schema maps directly:
- `entities` → Graphiti nodes
- `facts` → bi-temporal edges with `valid_from`/`valid_to`
- `superseded_by` → dedicated edge type

## Endpoints

### `GET /health`
Liveness probe. Returns `{ok, ingested, failed, pending}`.

### `POST /episode`
Enqueue an episode for async ingestion. Body: `{group_id, text, name?, source?, reference_time?}`.

### `POST /search`
Search facts. Body: `{group_id, query, limit?, as_of?}`.

### `POST /brief`
Memory brief for an entity. Body: `{group_id, center, limit?}`.

### `POST /baseline` (desk-specific)
Pre-draft validation. Body: `{group_id, deal_key, as_of?}`.
Returns:
```json
{
  "deal_key": "pluto",
  "as_of": "2026-08-26T23:10:00Z",
  "has_signed_contract": true,
  "contract_facts": [...],
  "open_asks": [...],
  "recommendation": "DO NOT ASK ABOUT SPECS — contract already signed/delivered"
}
```

### `POST /ingest/ledger`
Bulk ingest from ito.db. Query params: `group_id`, `db_path`.

## Ingestion

### From ito.db
- Obligations → entities with `has_status`, `owes`, `part_of`, `has_ask` facts
- Status transitions close old facts and open new ones
- Node-count asks extracted from summary text (e.g., "one H200 node" → `{"nodes": 1, "gpu": "h200"}`)
- Contract events detected from summary keywords + status

### From workstream-results
- JSON receipts with `obligation_id`, `status`, `superseded_by` close/open facts

## Pruning

The ingestion process is idempotent. Re-running `/ingest/ledger` updates facts based on current ledger state. Facts invalidated by newer events get `valid_to` set automatically.

## Usage

### Start the service
```bash
./plugins/temporal_knowledge/run-desk-service.sh
```

Or via launchd (persistent):
```bash
cp plugins/temporal_knowledge/com.hermes.temporal-knowledge-service.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.hermes.temporal-knowledge-service.plist
```

### Ingest the ledger
```bash
curl -X POST "http://127.0.0.1:8098/ingest/ledger?group_id=desk"
```

### Baseline check
```bash
curl -X POST http://127.0.0.1:8098/baseline \
  -H "Content-Type: application/json" \
  -d '{"group_id": "desk", "deal_key": "pluto", "as_of": "2026-08-26T23:10:00Z"}'
```

### As Hermes tools (gateway)
- `temporal_baseline_check(deal_key, as_of_ts?)` — verify current facts before drafting
- `temporal_ingest()` — refresh graph from ledger
- `temporal_brief(deal_key)` — memory brief

### CLI
```bash
hermes temporal check pluto
hermes temporal check pluto --as-of 1787785800
hermes temporal ingest
hermes temporal brief pluto
```

## Pluto/Ronit Case Study

**Approval 267** (filed at 19:10 ET, Aug 26 2026) asked Ronit:
> "can you confirm whether they need two or four nodes..."

**Reality at that moment:**
- Contract for **one** H200 node had been delivered at ~16:13 UTC (obligation 215)
- DocuSign envelope sent at ~20:34 UTC (obligation 233)
- The 2-node ask was already stale

**The service answers correctly:**
```json
{
  "has_signed_contract": true,
  "recommendation": "DO NOT ASK ABOUT SPECS — contract already signed/delivered"
}
```

## Verification

Run the acceptance test:
```bash
python3 plugins/temporal_knowledge/test_desk_graphiti.py
```

Expected output: `ALL ACCEPTANCE TESTS PASSED` with assertions that:
1. No signed contract before delivery timestamp
2. Signed contract detected after delivery
3. Approval 267 timestamp correctly flagged as stale
4. No open 2-node ask at 19:10 ET

## Files

- `desk_service.py` — FastAPI service (API-compatible with upstream)
- `ingest_ledger.py` — Ledger ingestion script
- `ontology.py` — Desk ontology (extends fleet pattern)
- `__init__.py` — Hermes plugin registration
- `run-desk-service.sh` — Service startup script
- `test_desk_graphiti.py` — Acceptance test
- `com.hermes.temporal-knowledge-service.plist` — launchd job

## Migration to Real Graphiti

When OpenAI/Nous credentials and Docker are available:

1. Start FalkorDB: `docker run -d --name ito-falkordb -p 6389:6379 falkordb/falkordb:latest`
2. Set `GRAPHITI_BACKEND=graphiti` and `OPENAI_API_KEY`
3. The service will swap the SQLite backend for graphiti-core with the same API
4. Re-ingest the ledger
