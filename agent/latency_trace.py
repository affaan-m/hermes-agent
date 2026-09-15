"""Per-turn latency tracing for the gateway -> agent -> model request path.

One ``LatencyTrace`` is opened when a platform message enters the gateway and
closed when the response is ready. Code along the path calls ``mark(label)``
at stage boundaries. On close, a single INFO line summarises the stage
durations, and an optional JSONL record is appended for offline analysis.

The trace travels two ways: a ``ContextVar`` (propagated into the gateway's
executor by ``copy_context``) and an ``agent._latency_trace`` attribute for
threads that do not inherit context (the ``_call`` API thread).

Environment:
  HERMES_LATENCY_TRACE=0          disable the summary log line (marks are still cheap no-ops)
  HERMES_LATENCY_TRACE_JSONL=path append one JSON record per turn to this file

Every public helper swallows its own errors. Tracing must never change the
behaviour of the request path.
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_current: "contextvars.ContextVar[Optional[LatencyTrace]]" = contextvars.ContextVar(
    "hermes_latency_trace", default=None
)


def _enabled() -> bool:
    return os.environ.get("HERMES_LATENCY_TRACE", "1").strip().lower() not in {
        "0", "false", "no", "off"
    }


def _jsonl_path() -> str:
    return os.environ.get("HERMES_LATENCY_TRACE_JSONL", "").strip()


class LatencyTrace:
    """Ordered list of (label, seconds since trace start, metadata)."""

    __slots__ = ("key", "t0", "wall0", "marks", "meta", "_lock", "closed")

    def __init__(self, key: str, **meta: Any) -> None:
        self.key = key
        self.t0 = time.perf_counter()
        self.wall0 = time.time()
        self.marks: List[Tuple[str, float, Dict[str, Any]]] = []
        self.meta: Dict[str, Any] = dict(meta)
        self._lock = threading.Lock()
        self.closed = False

    def mark(self, label: str, **meta: Any) -> float:
        t = time.perf_counter() - self.t0
        with self._lock:
            if self.closed:
                return t
            # Keep only the first occurrence of one-shot labels so a retry
            # cannot overwrite the first-token time the user experienced.
            if label.startswith("api.first_") and any(m[0] == label for m in self.marks):
                return t
            self.marks.append((label, t, dict(meta)))
        return t

    def elapsed(self) -> float:
        return time.perf_counter() - self.t0

    def stages(self) -> List[Tuple[str, float]]:
        """Duration of each stage, measured from the previous mark."""
        out: List[Tuple[str, float]] = []
        prev = 0.0
        for label, t, _ in self.marks:
            out.append((label, t - prev))
            prev = t
        return out

    def record(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "started_at": self.wall0,
            "total_s": round(self.elapsed(), 4),
            "meta": self.meta,
            "marks": [
                {"label": label, "t": round(t, 4), **({"meta": meta} if meta else {})}
                for label, t, meta in self.marks
            ],
        }

    def finish(self, **meta: Any) -> None:
        with self._lock:
            if self.closed:
                return
            self.closed = True
        self.meta.update(meta)
        total = self.elapsed()
        if _enabled():
            try:
                parts = ", ".join(f"{label}={dur * 1000:.0f}ms" for label, dur in self.stages())
                logger.info(
                    "latency trace: key=%s total=%.3fs %s stages: %s",
                    self.key,
                    total,
                    " ".join(f"{k}={v}" for k, v in self.meta.items()),
                    parts,
                )
            except Exception:
                pass
        path = _jsonl_path()
        if path:
            try:
                with open(path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(self.record(), default=str) + "\n")
            except Exception:
                pass


def start(key: str, **meta: Any) -> Optional[LatencyTrace]:
    """Open a new trace and bind it to the current context."""
    try:
        trace = LatencyTrace(key, **meta)
        _current.set(trace)
        return trace
    except Exception:
        return None


def start_if_missing(key: str, **meta: Any) -> Optional[LatencyTrace]:
    trace = current()
    if trace is not None and not trace.closed:
        return trace
    return start(key, **meta)


def current() -> Optional[LatencyTrace]:
    try:
        return _current.get()
    except Exception:
        return None


def bind(trace: Optional[LatencyTrace]) -> None:
    """Bind an existing trace to the current context (for non-inheriting threads)."""
    try:
        _current.set(trace)
    except Exception:
        pass


def attach(agent: Any, trace: Optional[LatencyTrace] = None) -> None:
    """Store the active trace on the agent so API worker threads can find it."""
    try:
        agent._latency_trace = trace if trace is not None else current()
    except Exception:
        pass


def _resolve(agent: Any = None) -> Optional[LatencyTrace]:
    trace = None
    if agent is not None:
        trace = getattr(agent, "_latency_trace", None)
    if trace is None or trace.closed:
        trace = current()
    if trace is None or trace.closed:
        return None
    return trace


def mark(label: str, agent: Any = None, **meta: Any) -> None:
    """Record a stage boundary on the active trace. No-op without a trace."""
    try:
        trace = _resolve(agent)
        if trace is not None:
            trace.mark(label, **meta)
    except Exception:
        pass


def finish(agent: Any = None, **meta: Any) -> None:
    try:
        trace = _resolve(agent)
        if trace is not None:
            trace.finish(**meta)
    except Exception:
        pass
