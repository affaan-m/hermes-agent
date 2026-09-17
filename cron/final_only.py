"""Cron final-only output policy.

This module is deliberately transport-agnostic. It runs immediately before the
cron delivery adapter and returns one cleaned, bounded message. It never makes
network calls and never invents a public URL for a local output file.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import re
from typing import Any, Mapping

try:
    import yaml
except ImportError:  # pragma: no cover - the runtime already bundles PyYAML
    yaml = None

_DEFAULT_MAX_CHARS = 3800

# These are router-owned lines, not report content. Matching is intentionally
# conservative and only applies to leading/trailing wrapper lines.
_LEADING_META = re.compile(
    r"^(?:scheduled\s+(?:task|job)\b.*|cron(?:job)?\s+response\b.*|"
    r"\(?job[_ -]?id\s*:.*|final\s+answer\s*:.*|"
    r"assistant\s+(?:response|final)\s*:.*|"
    r"(?:calling|running|executing|using)\s+(?:tool|function)\b.*|"
    r"tool\s+(?:call|output|result)\s*:.*|[-_=]{3,})\s*$",
    re.IGNORECASE,
)
_TRAILING_META = re.compile(
    r"^(?:to\s+stop\s+or\s+manage\s+this\s+job\b|"
    r"(?:job[_ -]?id|run[_ -]?id)\s*:|[-_=]{3,})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class FinalOnlyPolicy:
    enabled: bool = True
    max_chars: int = _DEFAULT_MAX_CHARS
    output_url_base: str | None = None


def is_log_job(job: Mapping[str, Any]) -> bool:
    """Do not change other profiles or deliveries outside the explicit logs lane."""
    path = Path(os.environ.get("ITO_ROUTING_CONFIG") or
                Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))) / "config/routing.yaml")
    if yaml is None or not path.is_file():
        return False
    try:
        data = yaml.safe_load(path.read_text()) or {}
        if data.get("scope") != "internal_ops_only":
            return False
        slack = data.get("channels", {}).get("slack", {}).get("logs", {}).get("channel_id")
        telegram = data.get("channels", {}).get("telegram", {}).get("logs", {})
        routes = {f"slack:{slack}"} if slack else set()
        if telegram.get("chat_id") and telegram.get("topic_id"):
            routes.add(f"telegram:{telegram['chat_id']}:{telegram['topic_id']}")
        return job.get("deliver") in routes
    except (OSError, ValueError, TypeError, AttributeError, yaml.YAMLError):
        return False


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _policy_section(data: Mapping[str, Any]) -> Mapping[str, Any]:
    """Accept the two deployed config shapes without coupling to route IDs."""
    for key in ("cron_final_only", "final_only", "cron"):
        section = data.get(key)
        if isinstance(section, Mapping):
            if key == "cron" and isinstance(section.get("final_only"), Mapping):
                return section["final_only"]
            return section
    return {}


def load_policy(path: str | Path | None = None) -> FinalOnlyPolicy:
    """Load policy from config/routing.yaml, failing closed to safe defaults."""
    config_path = Path(path or os.environ.get("ITO_ROUTING_CONFIG") or
                       Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))) / "config/routing.yaml")
    if not config_path.is_file() or yaml is None:
        return FinalOnlyPolicy()
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        section = _policy_section(raw if isinstance(raw, Mapping) else {})
        max_chars = int(section.get("max_chars", _DEFAULT_MAX_CHARS))
        if max_chars < 256:
            max_chars = 256
        max_chars = min(max_chars, _DEFAULT_MAX_CHARS)
        base = section.get("output_url_base") or section.get("full_output_url_base")
        # Only an explicitly configured HTTPS base may become a clickable URL.
        if base is not None and not str(base).strip().lower().startswith("https://"):
            base = None
        return FinalOnlyPolicy(
            enabled=_as_bool(section.get("enabled"), True),
            max_chars=max_chars,
            output_url_base=str(base).rstrip("/") if base else None,
        )
    except (OSError, ValueError, TypeError, yaml.YAMLError):
        return FinalOnlyPolicy()


def _strip_wrappers(text: str) -> str:
    text = re.sub(r"\A\s*\[IMPORTANT:\s*You are running as a scheduled[\s\S]*?\]\s*", "", str(text or ""), flags=re.I)
    text = re.sub(r"\A\s*<(?:think|analysis)>[\s\S]*?</(?:think|analysis)>\s*", "", text, flags=re.I)
    lines = [line.rstrip() for line in str(text or "").replace("\r\n", "\n").split("\n")]
    while lines and not lines[0].strip():
        lines.pop(0)
    # Remove only a contiguous leading router preamble. Do not scan/rewrite the
    # body, where a quoted tool transcript may be legitimate report evidence.
    while lines and (_LEADING_META.match(lines[0].strip()) or not lines[0].strip()):
        line = lines[0].strip()
        if re.match(r"^(?:final\s+answer|assistant\s+(?:response|final))\s*:", line, re.I):
            lines[0] = line.split(":", 1)[1].strip()
            if lines[0]:
                break
            lines.pop(0)
        else:
            lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    while lines and _TRAILING_META.match(lines[-1].strip()):
        lines.pop()
        while lines and not lines[-1].strip():
            lines.pop()
    # Cron logs are one text message. Attachments remain referenced in the full
    # saved output, not dispatched as additional platform messages.
    return re.sub(r"(?m)^\s*MEDIA:\S+\s*$", "", "\n".join(lines)).strip()


def _full_output_reference(path: str | Path | None, job: Mapping[str, Any], policy: FinalOnlyPolicy) -> str:
    if not path:
        return "Full output was saved locally by the cron scheduler."
    local = str(path)
    if policy.output_url_base:
        job_id = str(job.get("id") or Path(local).stem)
        return f"Full output: {policy.output_url_base}/{job_id}"
    return f"Full output saved locally: {local}"


def prepare_final_message(
    content: str,
    *,
    job: Mapping[str, Any] | None = None,
    full_output_path: str | Path | None = None,
    policy: FinalOnlyPolicy | None = None,
) -> str:
    """Return the sole outbound cron message after final-only filtering.

    The output is bounded to one message. When it is truncated, the suffix is
    kept inside the bound and references either an explicitly configured HTTPS
    artifact base or the local saved path. No ``file://`` or guessed public URL
    is emitted.
    """
    job = job or {}
    policy = policy or load_policy()
    cleaned = _strip_wrappers(content)
    if not policy.enabled or len(cleaned) <= policy.max_chars:
        return cleaned
    reference = "\n\n… [truncated] " + _full_output_reference(full_output_path, job, policy)
    if len(reference) >= policy.max_chars:
        reference = "\n\n… [truncated; full output saved by scheduler]"
    budget = max(0, policy.max_chars - len(reference))
    return cleaned[:budget].rstrip() + reference
