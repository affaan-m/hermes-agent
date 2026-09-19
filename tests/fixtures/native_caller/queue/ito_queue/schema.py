"""Task schema: one row per unit of work, with a checkable acceptance."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import time
from typing import Any

AREAS = ("ECC", "ITO", "PERSONAL", "INFRA")
KINDS = ("deterministic", "agentic", "human")
OWNERS = ("mini-claude", "mini-hermes", "pro-codex", "affaan")
STATES = ("open", "claimed", "blocked", "done", "dropped")
ACCEPTANCE_TYPES = ("shell", "receipt", "none")

ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,79}$")


class ValidationError(ValueError):
    pass


def now_iso(ts: float | None = None) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts if ts is not None else time.time()))


def new_id(title: str, source_ref: str = "", ts: float | None = None) -> str:
    """Content-derived ID; legacy ts is accepted but does not affect identity.

    Persisted explicit IDs are preserved; this does not migrate older generated IDs.
    """
    identity = json.dumps([source_ref, title], ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"q-{digest}"


@dataclasses.dataclass
class Acceptance:
    """How a task is proven done.

    shell:   manual diagnostic only; never automatic completion
    receipt: claim-bound v1 JSON proof; legacy text needs explicit reviewed completion
    none:    a human marks it done; no automatic check
    """

    type: str = "none"
    command: str = ""
    path: str = ""

    @classmethod
    def from_any(cls, value: Any) -> "Acceptance":
        if value is None or value == "":
            return cls()
        if isinstance(value, Acceptance):
            return value
        if isinstance(value, dict):
            acc = cls(
                type=str(value.get("type", "none")),
                command=str(value.get("command", "") or ""),
                path=str(value.get("path", "") or ""),
            )
        elif isinstance(value, str):
            text = value.strip()
            if text.startswith("receipt:"):
                acc = cls(type="receipt", path=text[len("receipt:"):].strip())
            elif text.startswith("shell:"):
                acc = cls(type="shell", command=text[len("shell:"):].strip())
            elif text.startswith("/") or text.startswith("~"):
                acc = cls(type="receipt", path=text)
            else:
                acc = cls(type="shell", command=text)
        else:
            raise ValidationError(f"acceptance must be a dict or string, got {type(value).__name__}")
        acc.validate()
        return acc

    def validate(self) -> None:
        if self.type not in ACCEPTANCE_TYPES:
            raise ValidationError(f"acceptance.type must be one of {ACCEPTANCE_TYPES}, got {self.type!r}")
        if self.type == "shell" and not self.command.strip():
            raise ValidationError("acceptance.type shell needs a command")
        if self.type == "receipt" and not self.path.strip():
            raise ValidationError("acceptance.type receipt needs a path")

    def to_dict(self) -> dict[str, str]:
        d = {"type": self.type}
        if self.command:
            d["command"] = self.command
        if self.path:
            d["path"] = self.path
        return d

    def short(self) -> str:
        if self.type == "shell":
            return f"shell: {self.command}"
        if self.type == "receipt":
            return f"receipt: {self.path}"
        return "none"


@dataclasses.dataclass
class Task:
    id: str
    title: str
    area: str
    kind: str
    owner: str
    state: str = "open"
    source_ref: str = ""
    detail: str = ""
    deps: list[str] = dataclasses.field(default_factory=list)
    acceptance: Acceptance = dataclasses.field(default_factory=Acceptance)
    receipts: list[str] = dataclasses.field(default_factory=list)
    blocked_on: str = ""
    priority: int = 50
    created_at: str = dataclasses.field(default_factory=now_iso)
    updated_at: str = dataclasses.field(default_factory=now_iso)
    claimed_by: str = ""
    revision: int = 0
    claim_token: str = ""
    last_check_at: str = ""
    last_check_ok: bool | None = None
    last_check_detail: str = ""

    def validate(self) -> None:
        if not ID_RE.match(self.id):
            raise ValidationError(f"bad id {self.id!r}: lowercase, digits, . _ -, 3 to 80 chars")
        if not self.title.strip():
            raise ValidationError(f"{self.id}: title is empty")
        if self.area not in AREAS:
            raise ValidationError(f"{self.id}: area must be one of {AREAS}, got {self.area!r}")
        if self.kind not in KINDS:
            raise ValidationError(f"{self.id}: kind must be one of {KINDS}, got {self.kind!r}")
        if self.owner not in OWNERS:
            raise ValidationError(f"{self.id}: owner must be one of {OWNERS}, got {self.owner!r}")
        if self.state not in STATES:
            raise ValidationError(f"{self.id}: state must be one of {STATES}, got {self.state!r}")
        if self.state == "blocked" and not self.blocked_on.strip():
            raise ValidationError(f"{self.id}: blocked tasks need blocked_on")
        if self.id in self.deps:
            raise ValidationError(f"{self.id}: a task cannot depend on itself")
        for dep in self.deps:
            if not ID_RE.match(dep):
                raise ValidationError(f"{self.id}: bad dep id {dep!r}")
        if not isinstance(self.priority, int) or not 0 <= self.priority <= 100:
            raise ValidationError(f"{self.id}: priority must be an int 0..100")
        if type(self.revision) is not int or self.revision < 0:
            raise ValidationError("revision must be a nonnegative integer")
        if not isinstance(self.claim_token, str):
            raise ValidationError("claim_token must be a string")
        self.acceptance.validate()
        if self.kind == "human" and self.acceptance.type == "shell":
            # A human task may still be checked by a command (a login that
            # makes a job healthy), so this is allowed; deterministic tasks
            # however must be checkable by machine.
            pass
        if self.kind == "deterministic" and self.acceptance.type == "none":
            raise ValidationError(f"{self.id}: a deterministic task needs a shell or receipt acceptance")

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Task":
        if not isinstance(d, dict):
            raise ValidationError("task must be a mapping")
        title = str(d.get("title", "")).strip()
        source_ref = str(d.get("source_ref", "") or "")
        tid = str(d.get("id") or new_id(title, source_ref))
        deps = d.get("deps") or []
        if isinstance(deps, str):
            deps = [x.strip() for x in deps.split(",") if x.strip()]
        receipts = d.get("receipts") or []
        if isinstance(receipts, str):
            receipts = [receipts]
        task = cls(
            id=tid,
            title=title,
            area=str(d.get("area", "")).upper(),
            kind=str(d.get("kind", "")).lower(),
            owner=str(d.get("owner", "")).lower(),
            state=str(d.get("state", "open")).lower(),
            source_ref=source_ref,
            detail=str(d.get("detail", "") or ""),
            deps=[str(x) for x in deps],
            acceptance=Acceptance.from_any(d.get("acceptance")),
            receipts=[str(x) for x in receipts],
            blocked_on=str(d.get("blocked_on", "") or ""),
            priority=int(d.get("priority", 50)),
            created_at=str(d.get("created_at") or now_iso()),
            updated_at=str(d.get("updated_at") or now_iso()),
            claimed_by=str(d.get("claimed_by", "") or ""),
            revision=d.get("revision", 0),
            claim_token=d.get("claim_token", ""),
            last_check_at=str(d.get("last_check_at", "") or ""),
            last_check_ok=d.get("last_check_ok"),
            last_check_detail=str(d.get("last_check_detail", "") or ""),
        )
        task.validate()
        return task

    def to_dict(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d["acceptance"] = self.acceptance.to_dict()
        return d
