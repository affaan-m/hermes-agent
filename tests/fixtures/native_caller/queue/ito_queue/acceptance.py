"""Run a task's acceptance check. Nothing here mutates the store."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import pathlib
import subprocess

from .schema import Acceptance, Task

SHELL_TIMEOUT_SEC = 60
_SECRET_MARKERS = ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "PRIVATE")


def _scrub(text: str) -> str:
    """Acceptance output can echo an env var by accident; drop any line that
    looks like it carries a secret name and value."""
    keep = []
    for line in text.splitlines():
        upper = line.upper()
        if "=" in line and any(m in upper for m in _SECRET_MARKERS):
            keep.append("[line withheld: looks like a secret assignment]")
        else:
            keep.append(line)
    return "\n".join(keep)


def check(acc: Acceptance, cwd: pathlib.Path | None = None) -> tuple[bool | None, str]:
    """Returns (ok, detail). ok is None when there is nothing to check."""
    if acc.type == "none":
        return None, "no automatic acceptance"
    if acc.type == "receipt":
        p = pathlib.Path(os.path.expanduser(acc.path))
        if p.is_file() and p.stat().st_size > 0:
            return True, f"receipt present ({p.stat().st_size} bytes)"
        return False, "receipt missing or empty"
    if acc.type == "shell":
        try:
            r = subprocess.run(
                acc.command, shell=True, capture_output=True, text=True,
                timeout=SHELL_TIMEOUT_SEC, cwd=str(cwd) if cwd else None,
            )
        except subprocess.TimeoutExpired:
            return False, f"timed out after {SHELL_TIMEOUT_SEC}s"
        except OSError as exc:
            return False, f"could not run: {type(exc).__name__}"
        tail = _scrub((r.stdout + r.stderr).strip())[-300:]
        return r.returncode == 0, f"rc={r.returncode}" + (f": {tail}" if tail else "")
    return None, f"unknown acceptance type {acc.type}"


RECEIPT_LIMIT = 65536


def _unique_keys(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate receipt field")
        value[key] = item
    return value


def check_task(task: Task, cwd: pathlib.Path | None = None) -> tuple[bool | None, str]:
    """Check a current claim's proof; no shell or human task runs automatically.

    This checks binding and reported outcome, not artifact truth or identity.
    The caller must finish_check with the SAME snapshot's revision/token.
    """
    if (task.state != "claimed" or task.kind == "human" or task.owner == "affaan"
            or task.claimed_by != task.owner or not task.claim_token):
        return None, "no eligible claim; explicit reviewed acceptance required"
    if task.acceptance.type != "receipt":
        return None, "legacy shell/none acceptance requires explicit reviewed completion"
    try:
        flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW
        fd = os.open(pathlib.Path(task.acceptance.path).expanduser(), flags)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= RECEIPT_LIMIT:
                return False, "receipt must be a bounded nonempty regular file"
            with os.fdopen(fd, "rb", closefd=False) as stream:
                data = stream.read(RECEIPT_LIMIT + 1)
        finally:
            os.close(fd)
        if len(data) > RECEIPT_LIMIT:
            return False, "receipt exceeds limit"
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_keys)
    except (OSError, ValueError, UnicodeError, RecursionError):
        return False, "receipt unavailable or not strict v1 JSON; explicit review required"
    expected = dict(version=1, task_id=task.id, owner=task.owner, revision=task.revision,
                    claim_token=task.claim_token, source_ref=task.source_ref, scope=task.title,
                    result="passed")
    if not isinstance(value, dict) or set(value) != set(expected) | {"evidence"}:
        return False, "receipt fields do not match v1 contract"
    if any(type(value[k]) is not type(v) or value[k] != v for k, v in expected.items()):
        return False, "receipt does not match current claim and scope"
    if not isinstance(value["evidence"], str) or not value["evidence"].strip():
        return False, "receipt needs an evidence reference"
    return True, "claim-bound receipt sha256=" + hashlib.sha256(data).hexdigest()
