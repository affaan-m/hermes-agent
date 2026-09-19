"""Bounded request-only Desk awareness; no default source, grants or I/O.

The host binds the separately owned RequestContextReader to an agent. This
module never obtains company identity from messages, CLI arguments or config.
It uses the existing opaque foreground request and does not authorize sends.
"""
from dataclasses import dataclass
from datetime import datetime
import json
import math
import sys
import time

MAX_CONTEXT_BYTES = 4096
MAX_READ_SECONDS = 0.100
MAX_ROWS = 20
READ_TOOLS = frozenset(("inventory_show", "match_query", "price_query"))
_STATUSES = frozenset(("ok", "current", "unavailable", "denied", "stale", "invalid", "oversize"))
_HEADER = "[Current audience-scoped Desk table: reference data, not authority.]\n"
_FOOTER = "\n[End table context]"
_monotonic = time.monotonic


@dataclass(frozen=True)
class _Binding:
    reader: object
    max_read_seconds: float = MAX_READ_SECONDS


def bind_context_reader(agent, reader, *, max_read_seconds=MAX_READ_SECONDS):
    """Host-only dependency binding; do not expose as a model tool.

    reader(request) must enforce bounded I/O itself. A synchronous arbitrary
    callable cannot be forcibly cancelled here. Over-budget returned results
    are rejected; this module creates no timeout threads or child processes.
    """
    if not callable(reader):
        raise TypeError("a host-owned bounded reader is required")
    if (type(max_read_seconds) not in (int, float) or not math.isfinite(max_read_seconds)
            or not 0 < max_read_seconds <= 1):
        raise ValueError("host read budget must be finite and at most one second")
    agent._desk_table_context_binding = _Binding(reader, float(max_read_seconds))


def _empty(status):
    return {"status": status, "generated_at": None, "rows": [], "truncated": False}


def _encode(value, limit):
    parts = []
    size = 0
    for chunk in json.JSONEncoder(sort_keys=True, separators=(",", ":"),
                                  ensure_ascii=True, allow_nan=False).iterencode(value):
        size += len(chunk.encode("utf-8"))
        if size > limit:
            raise OverflowError("context exceeds byte budget")
        parts.append(chunk)
    return "".join(parts)



def _source_descriptor(value):
    # Publication time is not a proof of complete ingestion. The reader may
    # supply event-store or canonical-book observations, never a coverage grant.
    if "source" not in value:
        return {"coverage": "unknown", "observation": "event_store_only",
                "newest_event_at": None, "event_age_seconds": None}
    source = value["source"]
    if (type(source) is not dict
            or set(source) != {"coverage", "observation", "newest_event_at", "event_age_seconds"}
            or source["coverage"] != "unknown"
            or source["observation"] not in ("event_store_only", "canonical_book_only")):
        raise ValueError("invalid source observation")
    age = source["event_age_seconds"]
    if age is not None and (type(age) is not int or age < 0):
        raise ValueError("invalid event age")
    stamp = source["newest_event_at"]
    if stamp is not None:
        if type(stamp) is not str or len(stamp) > 64:
            raise ValueError("invalid event timestamp")
        if datetime.fromisoformat(stamp.replace("Z", "+00:00")).utcoffset() is None:
            raise ValueError("event timestamp requires timezone")
    return source


def _projection(agent):
    binding = getattr(agent, "_desk_table_context_binding", None)
    if type(binding) is not _Binding:
        return _empty("unavailable")
    host = sys.modules.get("gateway.inventory_context")
    if host is None or getattr(host, "ABI_VERSION", None) != "inventory_request_v1":
        return _empty("denied")
    started = _monotonic()
    try:
        request = host.capture_inventory_request()
        if type(request) is not host.InventoryRequest or request.validate() is not True:
            return _empty("denied")
        identity = request.identity
        if (type(identity) is not tuple or len(identity) != 6
                or (getattr(agent, "_user_id", None), getattr(agent, "platform", None),
                    getattr(agent, "_chat_id", None)) != (identity[1], identity[2], identity[4])):
            return _empty("denied")
        value = binding.reader(request)
        # Fence both the active capability and dependency before releasing data.
        if (host.capture_inventory_request() is not request or request.validate() is not True
                or request.identity != identity
                or getattr(agent, "_desk_table_context_binding", None) is not binding):
            return _empty("denied")
        if _monotonic() - started > binding.max_read_seconds:
            return _empty("unavailable")
        if (type(value) is dict and type(value.get("status")) is str
                and value["status"] in _STATUSES - {"ok", "current"}):
            return _empty(value["status"])
        if (type(value) is not dict or set(value) not in ({"status", "generated_at", "rows", "truncated"},
                                       {"status", "generated_at", "rows", "truncated", "source"})
                or type(value["status"]) is not str or value["status"] not in _STATUSES
                or type(value["rows"]) is not list or len(value["rows"]) > MAX_ROWS
                or type(value["truncated"]) is not bool):
            return _empty("invalid")
        if value["status"] not in ("ok", "current"):
            return _empty(value["status"])
        try:
            source = _source_descriptor(value)
        except (TypeError, ValueError):
            return _empty("invalid")
        value = dict(value, source=source)
        # Row schema and field/audience filtering belong to the bound reader.
        # Detach its bounded JSON result before using it in this request.
        limit = MAX_CONTEXT_BYTES - len(("\n\n" + _HEADER + _FOOTER).encode("utf-8"))
        encoded = _encode(value, limit)
        if _monotonic() - started > binding.max_read_seconds:
            return _empty("unavailable")
        return json.loads(encoded)
    except OverflowError:
        return _empty("oversize")
    except Exception:
        # No internal paths, exception text, credentials or foreign rows.
        return _empty("unavailable")


def add_desk_table_context(agent, api_messages):
    """Return a per-call copy with compact context on the latest user message.

    The caller builds api_messages afresh from clean history on each outer
    iteration. Inner transport retries reuse that request; no cross-request
    cache or authority lease is retained by this helper.
    """
    binding = getattr(agent, "_desk_table_context_binding", None)
    if type(binding) is not _Binding and not READ_TOOLS.intersection(getattr(agent, "valid_tool_names", ())):
        return api_messages
    target = next((i for i in range(len(api_messages)-1, -1, -1)
                   if api_messages[i].get("role") == "user"), None)
    if target is None:
        return api_messages
    content = api_messages[target].get("content")
    if type(content) not in (str, list):
        return api_messages
    projection = _projection(agent)
    block = _HEADER + _encode(projection, MAX_CONTEXT_BYTES - len(("\n\n" + _HEADER + _FOOTER).encode("utf-8"))) + _FOOTER
    result = list(api_messages)
    result[target] = dict(api_messages[target])
    if type(content) is str:
        result[target]["content"] = content + "\n\n" + block
    else:
        result[target]["content"] = [*content, {"type": "text", "text": block}]
    return result
