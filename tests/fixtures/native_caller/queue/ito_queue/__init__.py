"""Deterministic task queue for the Itô desk fleet (mini, Pro, Air).

Every prompt Affaan sends becomes rows in one SQLite store on
/Volumes/Agent-Runtime/state/queue. Agents claim rows, acceptance checks
close them, and QUEUE.md is rendered for the Codex orchestrator on the Pro.
"""

from .schema import (  # noqa: F401
    AREAS,
    KINDS,
    OWNERS,
    STATES,
    Acceptance,
    Task,
    ValidationError,
    new_id,
)
from .store import Store, StoreError  # noqa: F401
