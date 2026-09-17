"""Conversation dispatch policy for the gateway.

Two knobs, both read from the ``gateway:`` section of config.yaml:

``gateway.per_conversation_serial`` (bool, default False)
    When True a message that arrives while its conversation (session key)
    already has a turn in flight is appended to that conversation's FIFO and
    runs as its own turn after the current one finishes. Nothing is
    interrupted, nothing is merged into one prompt. Arrival order is the
    turn order.

``gateway.max_concurrent_conversations`` (positive int, default unbounded)
    Caps how many conversations may run an agent turn at the same time,
    gateway wide (across every platform adapter). Conversations over the cap
    wait in a FIFO for a free slot. The per-conversation session guard is
    already held while waiting, so follow-ups for a waiting conversation
    queue behind it instead of starting a second turn.

Every dispatch writes one INFO line prefixed ``dispatch`` with the running
count, the cap, and how long the turn waited for a slot, so event loop
stalls can be read straight out of agent.log::

    dispatch seq=17 platform=slack session=slack:C123:thread:1.2 msg_id=1726.3 running=2/3 queued=0 waited_ms=0
    dispatch done seq=17 platform=slack session=... msg_id=1726.3 running=1/3 elapsed_ms=8412
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional

logger = logging.getLogger(__name__)


def coerce_max_concurrent_conversations(value: Any) -> Optional[int]:
    """Return a positive int cap or None (unbounded). Bad values mean None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        cap = int(value)
    except (TypeError, ValueError):
        logger.warning(
            "gateway.max_concurrent_conversations=%r is not an integer; treating as unbounded",
            value,
        )
        return None
    if cap <= 0:
        return None
    return cap


@dataclass
class DispatchSlot:
    """Handle for one running turn. Returned by ``acquire``; pass to ``release``."""

    seq: int
    session_key: str
    platform: str
    message_id: str
    started_at: float
    released: bool = field(default=False)


class ConversationDispatcher:
    """FIFO slot pool that bounds concurrent conversations."""

    def __init__(
        self,
        *,
        per_conversation_serial: bool = False,
        max_concurrent_conversations: Optional[int] = None,
    ) -> None:
        self.per_conversation_serial = bool(per_conversation_serial)
        self.max_concurrent_conversations = coerce_max_concurrent_conversations(
            max_concurrent_conversations
        )
        self._running: int = 0
        self._peak_running: int = 0
        self._seq: int = 0
        # (future, session_key) in arrival order. A waiter is woken by
        # release() handing it the slot directly, so wake order is FIFO
        # regardless of how the event loop schedules the callbacks.
        self._waiters: Deque[asyncio.Future] = deque()
        # session_key -> number of live slots held. A conversation that
        # already holds a slot (its drain task starting while the parent
        # task unwinds) re-enters without consuming a second one, so the
        # count is per conversation, not per asyncio task.
        self._holders: Dict[str, int] = {}

    # ----------------------------------------------------------------- state

    @property
    def running(self) -> int:
        return self._running

    @property
    def peak_running(self) -> int:
        return self._peak_running

    @property
    def queued(self) -> int:
        return len(self._waiters)

    @property
    def serial(self) -> bool:
        return self.per_conversation_serial

    def _cap_label(self) -> str:
        cap = self.max_concurrent_conversations
        return str(cap) if cap is not None else "inf"

    # ------------------------------------------------------------- lifecycle

    async def acquire(
        self,
        session_key: str,
        *,
        platform: str = "?",
        message_id: Any = None,
    ) -> DispatchSlot:
        """Wait for a free conversation slot, then log the dispatch."""
        self._seq += 1
        seq = self._seq
        wait_started = time.monotonic()
        cap = self.max_concurrent_conversations
        if session_key in self._holders:
            self._holders[session_key] += 1
        elif cap is not None and (self._running >= cap or self._waiters):
            loop = asyncio.get_running_loop()
            fut: asyncio.Future = loop.create_future()
            self._waiters.append(fut)
            logger.info(
                "dispatch wait seq=%d platform=%s session=%s msg_id=%s running=%d/%s queued=%d",
                seq, platform, session_key, message_id, self._running,
                self._cap_label(), len(self._waiters),
            )
            try:
                await fut
            except asyncio.CancelledError:
                # Cancelled while waiting: drop our place. If release()
                # already handed us the slot, pass it on so it is not lost.
                if fut in self._waiters:
                    self._waiters.remove(fut)
                elif fut.done() and not fut.cancelled():
                    # Ownership already moved to us; hand it on unchanged.
                    self._release_count()
                raise
            self._holders[session_key] = 1
        else:
            self._running += 1
            self._holders[session_key] = 1
        self._peak_running = max(self._peak_running, self._running)
        waited_ms = int((time.monotonic() - wait_started) * 1000)
        slot = DispatchSlot(
            seq=seq,
            session_key=session_key,
            platform=platform,
            message_id=str(message_id) if message_id is not None else "",
            started_at=time.monotonic(),
        )
        logger.info(
            "dispatch seq=%d platform=%s session=%s msg_id=%s running=%d/%s queued=%d waited_ms=%d",
            seq, platform, session_key, slot.message_id, self._running,
            self._cap_label(), len(self._waiters), waited_ms,
        )
        return slot

    def _release_count(self) -> None:
        """Decrement running, or hand the slot straight to the next waiter."""
        while self._waiters:
            fut = self._waiters.popleft()
            if fut.done():
                continue
            # Ownership transfers to the waiter: running stays the same.
            fut.set_result(None)
            return
        self._running = max(0, self._running - 1)

    def release(self, slot: Optional[DispatchSlot]) -> None:
        if slot is None or slot.released:
            return
        slot.released = True
        remaining = self._holders.get(slot.session_key, 1) - 1
        if remaining > 0:
            self._holders[slot.session_key] = remaining
        else:
            self._holders.pop(slot.session_key, None)
            self._release_count()
        elapsed_ms = int((time.monotonic() - slot.started_at) * 1000)
        logger.info(
            "dispatch done seq=%d platform=%s session=%s msg_id=%s running=%d/%s elapsed_ms=%d",
            slot.seq, slot.platform, slot.session_key, slot.message_id,
            self._running, self._cap_label(), elapsed_ms,
        )


# ------------------------------------------------------------- module state

_dispatcher: Optional[ConversationDispatcher] = None


def get_dispatcher() -> ConversationDispatcher:
    """Return the process wide dispatcher (unbounded, non-serial until configured)."""
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = ConversationDispatcher()
    return _dispatcher


def configure_dispatcher(
    *,
    per_conversation_serial: bool = False,
    max_concurrent_conversations: Optional[int] = None,
) -> ConversationDispatcher:
    """Install a fresh dispatcher with the given policy. Called by GatewayRunner."""
    global _dispatcher
    _dispatcher = ConversationDispatcher(
        per_conversation_serial=per_conversation_serial,
        max_concurrent_conversations=max_concurrent_conversations,
    )
    logger.info(
        "dispatch policy per_conversation_serial=%s max_concurrent_conversations=%s",
        _dispatcher.per_conversation_serial,
        _dispatcher._cap_label(),
    )
    return _dispatcher


def reset_dispatcher() -> None:
    """Drop the module dispatcher (tests)."""
    global _dispatcher
    _dispatcher = None
