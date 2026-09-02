"""Fast path for trivial turns (prototype).

A trivial turn is a short, plain inbound message ("ok thanks", "yes go
ahead", "what's the status?") that the model almost always answers directly
without tools. Live profiling of the Ito desk gateway showed the model call
itself is roughly 80% of a trivial turn's wall time, and the rare 100s+ tails
come from a stream that never sends a first byte inside the default 120s
watchdog window. Hermes' own in-process work is under 100ms.

The fast path therefore changes only the first model call of a trivial turn:

  * reasoning effort is lowered (default ``low``) for that call only. The
    system prompt, tools and history are untouched, so the provider prompt
    cache prefix is preserved.
  * the no-first-byte cutoff is tightened (default 25s) so a wedged stream
    is reconnected quickly instead of after 120s.

If the model still decides to call tools, the loop continues exactly as
before at the normal effort. Misclassification therefore costs at most one
lower-effort call.

Configuration (environment first, then ``fast_path:`` in config.yaml):

  HERMES_FAST_PATH=1|0                 enable (default off)
  HERMES_FAST_PATH_EFFORT=low          reasoning effort for the trivial call
  HERMES_FAST_PATH_TTFB=25             seconds to wait for a first byte
  HERMES_FAST_PATH_MAX_CHARS=160       longest message treated as trivial
  HERMES_FAST_PATH_MAX_WORDS=24
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_DEFAULTS: Dict[str, Any] = {
    "enabled": False,
    "reasoning_effort": "low",
    "ttfb_seconds": 25.0,
    "max_chars": 160,
    "max_words": 24,
}

_VALID_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}

# Gateway/platform wrappers that are prepended to the user's text.
_WRAPPER_LINE = re.compile(r"^\[(Replying to|Thread context|Forwarded|Quoted)[^\]]*\].*$", re.IGNORECASE)
_ATTACHMENT = re.compile(r"\[(image|file|voice|audio|video|document|sticker|attachment)", re.IGNORECASE)
_URL = re.compile(r"https?://", re.IGNORECASE)
_CODE_FENCE = "```"


@dataclass(frozen=True)
class FastPathPlan:
    reasoning_effort: str
    ttfb_seconds: float
    reason: str

    def as_dict(self) -> Dict[str, Any]:
        return {"effort": self.reasoning_effort, "ttfb": self.ttfb_seconds, "reason": self.reason}


_config_cache: Optional[Dict[str, Any]] = None


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_config(force: bool = False) -> Dict[str, Any]:
    """Merge defaults, ``fast_path:`` from config.yaml, then environment."""
    global _config_cache
    if _config_cache is not None and not force:
        return _config_cache
    cfg = dict(_DEFAULTS)
    try:
        from hermes_cli.config import load_config as _load_hermes_config

        block = (_load_hermes_config() or {}).get("fast_path")
        if isinstance(block, dict):
            for key in cfg:
                if key in block and block[key] is not None:
                    cfg[key] = block[key]
    except Exception:
        pass
    env = os.environ
    if "HERMES_FAST_PATH" in env:
        cfg["enabled"] = _truthy(env["HERMES_FAST_PATH"])
    if env.get("HERMES_FAST_PATH_EFFORT"):
        cfg["reasoning_effort"] = env["HERMES_FAST_PATH_EFFORT"].strip().lower()
    for env_key, cfg_key, cast in (
        ("HERMES_FAST_PATH_TTFB", "ttfb_seconds", float),
        ("HERMES_FAST_PATH_MAX_CHARS", "max_chars", int),
        ("HERMES_FAST_PATH_MAX_WORDS", "max_words", int),
    ):
        if env.get(env_key):
            try:
                cfg[cfg_key] = cast(env[env_key])
            except ValueError:
                pass
    if cfg["reasoning_effort"] not in _VALID_EFFORTS:
        cfg["reasoning_effort"] = _DEFAULTS["reasoning_effort"]
    try:
        cfg["ttfb_seconds"] = float(cfg["ttfb_seconds"])
    except (TypeError, ValueError):
        cfg["ttfb_seconds"] = _DEFAULTS["ttfb_seconds"]
    _config_cache = cfg
    return cfg


def reset_config_cache() -> None:
    global _config_cache
    _config_cache = None


def strip_gateway_wrappers(text: str) -> str:
    """Drop reply/thread context lines the gateway prepends to the user text."""
    kept = []
    for line in (text or "").splitlines():
        if _WRAPPER_LINE.match(line.strip()):
            continue
        kept.append(line)
    return "\n".join(kept).strip()


def classify_message(text: str, cfg: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Return None when the message is trivial, otherwise the reason it is not."""
    cfg = cfg or load_config()
    body = strip_gateway_wrappers(text)
    if not body:
        return "empty"
    if len(body) > int(cfg["max_chars"]):
        return "too_long"
    if len(body.split()) > int(cfg["max_words"]):
        return "too_many_words"
    if _CODE_FENCE in body:
        return "code"
    if _URL.search(body):
        return "url"
    if _ATTACHMENT.search(body):
        return "attachment"
    if body.count("\n") >= 3:
        return "multiline"
    return None


def plan_for_turn(agent: Any, user_message: str) -> Optional[FastPathPlan]:
    """Decide whether this turn takes the fast path and remember it on the agent."""
    plan: Optional[FastPathPlan] = None
    try:
        cfg = load_config()
        if cfg.get("enabled"):
            why_not = classify_message(user_message, cfg)
            if why_not is None:
                plan = FastPathPlan(
                    reasoning_effort=str(cfg["reasoning_effort"]),
                    ttfb_seconds=float(cfg["ttfb_seconds"]),
                    reason="trivial_message",
                )
                logger.info(
                    "fast path: trivial turn -> first call uses reasoning effort=%s, first-byte cutoff=%.0fs",
                    plan.reasoning_effort, plan.ttfb_seconds,
                )
            else:
                logger.debug("fast path: not trivial (%s)", why_not)
    except Exception:
        logger.debug("fast path planning failed", exc_info=True)
        plan = None
    try:
        agent._fast_path_plan = plan
    except Exception:
        pass
    return plan


def active_plan(agent: Any) -> Optional[FastPathPlan]:
    """The plan applies to the first model call of the turn only."""
    plan = getattr(agent, "_fast_path_plan", None)
    if plan is None:
        return None
    if int(getattr(agent, "_api_call_count", 0) or 0) > 1:
        return None
    return plan


def effective_reasoning_config(agent: Any) -> Optional[Dict[str, Any]]:
    """``agent.reasoning_config`` with the fast-path effort applied when active."""
    base = getattr(agent, "reasoning_config", None)
    plan = active_plan(agent)
    if plan is None:
        return base
    if isinstance(base, dict) and base.get("enabled") is False:
        return base
    merged = dict(base) if isinstance(base, dict) else {}
    merged["effort"] = plan.reasoning_effort
    return merged


def first_byte_cutoff(agent: Any, default: Optional[float]) -> Optional[float]:
    """Tighten a no-first-byte cutoff when the fast path is active."""
    plan = active_plan(agent)
    if plan is None:
        return default
    if default is None or default <= 0:
        return plan.ttfb_seconds
    return min(float(default), plan.ttfb_seconds)
