"""ito-queue: add, list, claim, done, block, unblock, drop, show, import,
intake, check, render, events."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

from . import intake, render
from .acceptance import check_task
from .schema import AREAS, KINDS, OWNERS, Acceptance, Task, ValidationError
from .store import Store, StoreError

REPO = pathlib.Path(__file__).resolve().parents[2]


def _actor(args) -> str:
    actor = getattr(args, "as_", None) or os.environ.get("ITO_QUEUE_ACTOR", "")
    if not actor:
        raise SystemExit("who is acting? pass --as <owner> or set ITO_QUEUE_ACTOR")
    if actor not in OWNERS:
        raise SystemExit(f"--as must be one of {OWNERS}")
    return actor


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ito-queue", description=__doc__)
    p.add_argument("--dir", help="store directory (default: /Volumes/Agent-Runtime/state/queue or $ITO_QUEUE_DIR)")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("add", help="add one task")
    a.add_argument("title")
    a.add_argument("--area", required=True, choices=AREAS)
    a.add_argument("--kind", required=True, choices=KINDS)
    a.add_argument("--owner", required=True, choices=OWNERS)
    a.add_argument("--id")
    a.add_argument("--source", default="", help="message ref (file path, message id, URL)")
    a.add_argument("--detail", default="")
    a.add_argument("--dep", action="append", default=[], help="task id this depends on (repeatable)")
    a.add_argument("--accept", default="", help="'shell: cmd', 'receipt: /path', or bare path/command")
    a.add_argument("--priority", type=int, default=50)
    a.add_argument("--as", dest="as_")

    ls = sub.add_parser("list", help="list open tasks")
    ls.add_argument("--state", choices=("open", "claimed", "blocked", "done", "dropped"))
    ls.add_argument("--area", choices=AREAS)
    ls.add_argument("--owner", choices=OWNERS)
    ls.add_argument("--all", action="store_true", help="include done and dropped")
    ls.add_argument("--json", action="store_true")

    for name, help_ in (("reassign", "release/reassign a current task with revision CAS"), ("claim", "claim a task"), ("done", "mark done"), ("block", "block with a reason"),
                        ("unblock", "reopen a blocked task"), ("drop", "drop with a reason"),
                        ("show", "print one task"), ("check", "run acceptance for one task")):
        s = sub.add_parser(name, help=help_)
        s.add_argument("id")
        if name in ("claim", "done", "block", "unblock", "drop", "reassign"):
            s.add_argument("--as", dest="as_")
        if name in ("done", "block", "unblock", "drop", "reassign"):
            s.add_argument("--revision", type=int, help="current task revision from show/claim")
        if name == "reassign":
            s.add_argument("--owner", required=True, choices=OWNERS)
            s.add_argument("--reason", required=True)
        if name == "done":
            s.add_argument("--reviewed", action="store_true", help="assert explicit operator review; not authentication")
            s.add_argument("--receipt", default="", help="receipt path or note")
        if name in ("block", "drop"):
            s.add_argument("reason")

    imp = sub.add_parser("import", help="import tasks from a YAML or JSON seed file")
    imp.add_argument("file")
    imp.add_argument("--replace", action="store_true", help="removed: fails closed; use explicit revision-bound transitions")
    imp.add_argument("--as", dest="as_")

    it = sub.add_parser("intake", help="split a pasted message into proposed tasks")
    it.add_argument("file", help="path to the message text, or - for stdin")
    it.add_argument("--source", default="", help="message ref recorded on every task (default: the file path)")
    it.add_argument("--area", default="ITO", choices=AREAS, help="default area when no hint is found")
    it.add_argument("--llm", choices=("none", "anthropic"), default="none",
                    help="optional naming and classification pass (needs anthropic SDK and credential)")
    it.add_argument("--model", default="claude-opus-5")
    it.add_argument("--receipt-dir", default=str(pathlib.Path.home() / ".codex" / "workstream-results" / "queue-receipts"))
    it.add_argument("--write", action="store_true", help="write the proposals after showing them")
    it.add_argument("--yes", action="store_true", help="with --write: do not prompt for confirmation")
    it.add_argument("--as", dest="as_")

    r = sub.add_parser("render", help="write QUEUE.md (and check claimed tasks unless --no-check)")
    r.add_argument("--out", help="output path (default ~/.codex/workstream-results/QUEUE.md or $ITO_QUEUE_MD)")
    r.add_argument("--no-check", action="store_true")

    ev = sub.add_parser("events", help="recent audit events")
    ev.add_argument("--id")
    ev.add_argument("--limit", type=int, default=30)
    return p


def _print_task(t: Task) -> None:
    print(json.dumps(t.to_dict(), indent=2))


def _load_seed(path: pathlib.Path) -> list[Task]:
    text = path.read_text()
    if path.suffix in (".yaml", ".yml"):
        import yaml
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    if isinstance(data, dict) and "tasks" in data:
        defaults = {k: v for k, v in data.items() if k != "tasks"}
        data = [{**defaults, **row} for row in data["tasks"]]
    if not isinstance(data, list):
        raise ValidationError("seed must be a list of tasks or {defaults..., tasks: [...]}")
    return [Task.from_dict(row) for row in data]


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        store = Store(args.dir)
    except StoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        return _dispatch(args, store)
    except (StoreError, ValidationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        store.close()


def _dispatch(args, store: Store) -> int:
    cmd = args.cmd
    if cmd == "add":
        task = Task.from_dict({
            "id": args.id, "title": args.title, "area": args.area, "kind": args.kind, "owner": args.owner,
            "source_ref": args.source, "detail": args.detail, "deps": args.dep,
            "acceptance": args.accept, "priority": args.priority,
        })
        store.add(task, actor=getattr(args, "as_", "") or os.environ.get("ITO_QUEUE_ACTOR", ""))
        print(task.id)
        return 0
    if cmd == "list":
        tasks = store.list(state=args.state, area=args.area, owner=args.owner, include_closed=args.all)
        if args.json:
            print(json.dumps([t.to_dict() for t in tasks], indent=2))
            return 0
        print(f"{'STATE':10s} {'ID':20s} {'AREA':8s} {'KIND':13s} {'OWNER':12s} PRI TITLE")
        for t in tasks:
            print(f"{t.state:10s} {t.id:20s} {t.area:8s} {t.kind:13s} {t.owner:12s} {t.priority:3d} {t.title[:70]}")
        print(f"\n{len(tasks)} task(s)")
        return 0
    if cmd == "show":
        t = store.get(args.id)
        if not t:
            raise StoreError(f"no task {args.id}")
        _print_task(t)
        return 0
    if cmd == "claim":
        _print_task(store.claim(args.id, _actor(args)))
        return 0
    if cmd == "reassign":
        _print_task(store.reassign(args.id, _actor(args), args.owner, expected_revision=args.revision, reason=args.reason))
        return 0
    if cmd == "done":
        _print_task(store.done(args.id, _actor(args), receipt=args.receipt, expected_revision=args.revision, reviewed=args.reviewed))
        return 0
    if cmd == "block":
        _print_task(store.block(args.id, _actor(args), args.reason, expected_revision=args.revision))
        return 0
    if cmd == "unblock":
        _print_task(store.unblock(args.id, _actor(args), expected_revision=args.revision))
        return 0
    if cmd == "drop":
        _print_task(store.drop(args.id, _actor(args), args.reason, expected_revision=args.revision))
        return 0
    if cmd == "check":
        t = store.get(args.id)
        if not t:
            raise StoreError(f"no task {args.id}")
        ok, detail = check_task(t, cwd=REPO)
        print(f"{'pass' if ok else 'n/a' if ok is None else 'FAIL'}: {detail}")
        return 0 if ok else 1
    if cmd == "import":
        tasks = _load_seed(pathlib.Path(args.file).expanduser())
        added, skipped = store.import_tasks(tasks, actor=getattr(args, "as_", "") or "import", replace=args.replace)
        print(f"imported {added}, skipped {skipped} existing")
        return 0
    if cmd == "intake":
        text = sys.stdin.read() if args.file == "-" else pathlib.Path(args.file).expanduser().read_text()
        source = args.source or (args.file if args.file != "-" else "stdin")
        proposals = intake.propose(text, default_area=args.area)
        if args.llm == "anthropic":
            proposals = intake.apply_llm_names(proposals, model=args.model)
        print(intake.format_proposals(proposals))
        if not proposals or not args.write:
            if proposals and not args.write:
                print("\n(dry run: add --write to store these)")
            return 0
        if not args.yes:
            answer = input(f"\nwrite {len(proposals)} task(s) to {store.path}? [y/N] ").strip().lower()
            if answer not in ("y", "yes"):
                print("not written")
                return 0
        tasks = [p.to_task(source, receipt_dir=args.receipt_dir) for p in proposals]
        added, skipped = store.import_tasks(tasks, actor=getattr(args, "as_", "") or "intake")
        print(f"written {added}, skipped {skipped} existing")
        return 0
    if cmd == "render":
        if not args.no_check:
            for task, ok, detail in render.check_claimed(store, cwd=REPO):
                tag = "pass" if ok else "n/a" if ok is None else "FAIL"
                print(f"check {task.id}: {tag} {detail}")
        out = render.write(store, pathlib.Path(args.out).expanduser() if args.out else None)
        print(f"wrote {out}")
        return 0
    if cmd == "events":
        for e in store.events(args.id, args.limit):
            print(f"{e['ts']} {e['task_id']:20s} {e['event']:10s} {e['actor']:12s} {e['detail'][:80]}")
        return 0
    raise SystemExit(f"unknown command {cmd}")
