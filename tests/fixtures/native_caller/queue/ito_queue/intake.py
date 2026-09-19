"""Turn a pasted message into proposed tasks.

Order is fixed: deterministic splitting first (sentences, then imperative
clauses), then an optional model pass that only names and classifies the
pieces it is handed. The model never decides what the tasks are; it cannot
add, merge or drop a piece. Nothing is written until the caller confirms.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from .schema import AREAS, KINDS, OWNERS, Acceptance, Task, new_id

# ---------------------------------------------------------------- splitting

_SENTENCE_END = re.compile(r"(?<=[.!?])\s+|\n+")
# Clause separators that start a new imperative inside one run-on sentence.
_VERBS = (
    "build", "create", "make", "write", "add", "fix", "ship", "merge", "deploy", "set up", "setup",
    "check", "run", "move", "migrate", "wire", "send", "reply", "respond", "draft", "schedule",
    "reschedule", "rebook", "confirm", "tell", "ask", "review", "audit", "test", "connect",
    "replicate", "begin", "start", "stop", "remove", "delete", "update", "rename", "install",
)
# A comma followed by a fresh imperative, a "we need"/"it should" clause, or a
# "for example" starts a new piece inside one run-on sentence.
_CLAUSE_SPLIT = re.compile(
    r",\s+(?=(?:and\s+|then\s+|also\s+|plus\s+|so\s+)?(?:"
    + "|".join(_VERBS)
    + r"|(?:i|we|you|it|this|that|things|automations|my\s+focus|the\s+\w+\s+(?:stuff|agent|tools?|system))"
    r"\s+(?:need|needs|should|must|might|can'?t|cannot|have\s+to|has\s+to|is\s+now|like)"
    r"|for\s+example"
    r"|does\s+this)\b)",
    re.I,
)
_IMPERATIVE_LEAD = re.compile(
    r"^(?:please\s+|so\s+|and\s+|then\s+|also\s+|plus\s+|now\s+)*"
    r"(?:build|create|make|write|add|fix|ship|merge|deploy|set\s*up|check|run|move|migrate|wire|send|"
    r"reply|respond|draft|schedule|reschedule|rebook|confirm|tell|ask|review|audit|test|connect|"
    r"replicate|begin|start|stop|remove|delete|update|rename|install|seed|turn|split|combine|use)\b",
    re.I,
)
_NEED = re.compile(
    r"\b(?:i\s+need|we\s+need|you\s+need|need\s+to|needs?\s+to\s+exist|should(?:'ve|\s+have)?|must|"
    r"has\s+to|have\s+to|it\s+might\s+(?:also\s+)?be\s+useful|should\s+also|the\s+best\s+tests?\s+are|"
    r"my\s+focus\s+is|begin\s+the\s+creation|from\s+now\s+on)\b",
    re.I,
)
_NOISE = re.compile(
    r"^(?:ok|okay|yeah|yes|no|lol|lmao|hopefully|does\s+this\s+all\s+make\s+sense|you\s+can\s+see\s+why|"
    r"i\s+can'?t\s+believe|i\s+am\s+the|i\s+don'?t\s+lose|there'?s\s+no\s+missing\s+context)",
    re.I,
)
MIN_WORDS = 4


@dataclass
class Piece:
    text: str
    index: int
    imperative: bool


def split_message(text: str) -> list[Piece]:
    """Sentences, then imperative clauses within them. Deterministic."""
    pieces: list[Piece] = []
    for raw in _SENTENCE_END.split(text.strip()):
        sentence = raw.strip()
        if not sentence:
            continue
        for clause in _CLAUSE_SPLIT.split(sentence):
            clause = clause.strip(" ,;")
            if len(clause.split()) < MIN_WORDS:
                continue
            imperative = bool(_IMPERATIVE_LEAD.match(clause) or _NEED.search(clause))
            pieces.append(Piece(text=clause, index=len(pieces), imperative=imperative))
    return pieces


def actionable(pieces: list[Piece]) -> list[Piece]:
    return [p for p in pieces if p.imperative and not _NOISE.match(p.text)]


# ----------------------------------------------------------- classification

_AREA_HINTS = {
    "ECC": ("ecc", "agentshield", "npm", "sponsor", "haley", "serpapi", "github sponsors", "harness"),
    "ITO": ("itô", "ito", "desk", "rfq", "inventory", "matching engine", "sequoia", "jump", "investor",
            "supplier", "buyer", "gpu", "hermes", "telegram", "slack", "deal"),
    "INFRA": ("aws", "launchd", "mini", "machine", "cloud agent", "migration", "queue", "disk", "reboot",
              "cron", "cherry pick", "cherry-pick", "upstream", "codebase"),
    "PERSONAL": ("personal", "imessage", "jet lag", "surgery", "my mail", "rescheduled"),
}
_HUMAN_HINTS = ("approve", "approval", "sign", "login", "log in", "qr", "sudo", "2fa", "pay", "invoice",
                "meeting", "in person", "call ", "rebook", "reschedule", "tell ", "decide")
_DETERMINISTIC_HINTS = ("cron", "job", "rule", "rules", "queue", "cherry", "script", "notification",
                        "notifications", "organiz", "launchd", "render", "deterministic")


def classify(piece: Piece, default_area: str = "ITO") -> dict:
    low = piece.text.lower()
    scores = {a: sum(low.count(h) for h in hints) for a, hints in _AREA_HINTS.items()}
    area = max(scores, key=lambda a: (scores[a], a == default_area))
    if scores[area] == 0:
        area = default_area
    if any(h in low for h in _HUMAN_HINTS):
        kind, owner = "human", "affaan"
    elif any(h in low for h in _DETERMINISTIC_HINTS):
        kind, owner = "deterministic", "mini-claude"
    else:
        kind, owner = "agentic", "pro-codex" if area == "ECC" else "mini-claude"
    if "hermes" in low and kind != "human":
        owner = "mini-hermes"
    title = _heuristic_title(piece.text)
    return {"title": title, "area": area, "kind": kind, "owner": owner}


def _heuristic_title(text: str) -> str:
    t = re.sub(r"^(?:please\s+|so\s+|and\s+|then\s+|also\s+|plus\s+|now\s+)+", "", text.strip(), flags=re.I)
    t = re.sub(r"^(?:i\s+need\s+you\s+to|i\s+need\s+to|we\s+need\s+to|you\s+need\s+to|need\s+to)\s+", "", t, flags=re.I)
    words = t.split()
    title = " ".join(words[:12])
    if len(words) > 12:
        title += " ..."
    return title[:1].upper() + title[1:]


# ------------------------------------------------------------- proposals

@dataclass
class Proposal:
    piece: Piece
    title: str
    area: str
    kind: str
    owner: str
    acceptance: Acceptance = field(default_factory=Acceptance)
    named_by: str = "heuristic"

    def to_task(self, source_ref: str, receipt_dir: str = "") -> Task:
        fragment_ref = f"{source_ref}#{self.piece.index}"
        task_id = new_id(self.title, fragment_ref)
        acc = self.acceptance
        if acc.type == "none" and self.kind == "deterministic":
            acc = Acceptance(type="receipt", path=f"{receipt_dir.rstrip('/')}/{task_id}-RECEIPT.md" if receipt_dir else "")
            if not acc.path:
                acc = Acceptance()
                self.kind = "agentic"
        return Task(
            id=task_id,
            title=self.title,
            area=self.area,
            kind=self.kind,
            owner=self.owner,
            source_ref=fragment_ref,
            detail=self.piece.text,
            acceptance=acc,
        )


def propose(text: str, default_area: str = "ITO") -> list[Proposal]:
    out = []
    for piece in actionable(split_message(text)):
        c = classify(piece, default_area=default_area)
        out.append(Proposal(piece=piece, **c))
    return out


# --------------------------------------------------------------- LLM pass

LLM_SYSTEM = (
    "You name and classify task fragments for an engineering task queue. For each fragment "
    "return a short imperative title (max 12 words, no em dashes), an area, a kind and an owner. "
    f"area is one of {list(AREAS)}. kind is one of {list(KINDS)}: deterministic means a script or "
    "rule can do it and a command can verify it; agentic means an agent must reason; human means "
    "only a person can do it (approvals, logins, money, meetings, outbound messages to people). "
    f"owner is one of {list(OWNERS)}: pro-codex owns ECC repo work and ito-cloud-runtime source; "
    "mini-claude owns ito-desk and mini automation; mini-hermes owns the Hermes gateway lanes; "
    "affaan is the human. Return only JSON: a list with one object per fragment, in the same order, "
    "with keys index, title, area, kind, owner. Never add, merge or drop fragments."
)


def apply_llm_names(proposals: list[Proposal], model: str = "claude-opus-5") -> list[Proposal]:
    """Optional: rename and reclassify with Claude. Requires the anthropic SDK
    and a credential in the environment (run under ito-env-run). Structure
    stays fixed: the model may only change title/area/kind/owner per index."""
    if not proposals:
        return proposals
    import anthropic  # local import: the desk venv does not always carry it

    client = anthropic.Anthropic()
    payload = [{"index": i, "text": p.piece.text} for i, p in enumerate(proposals)]
    response = client.messages.create(
        model=model,
        max_tokens=4096,
        system=LLM_SYSTEM,
        messages=[{"role": "user", "content": json.dumps(payload)}],
        output_config={"effort": "low"},
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("model refused the naming pass; keep the heuristic names")
    text = "".join(block.text for block in response.content if getattr(block, "type", "") == "text")
    return merge_llm_output(proposals, text)


def merge_llm_output(proposals: list[Proposal], text: str) -> list[Proposal]:
    """Validate the model's JSON against the fixed proposal list. Anything
    malformed falls back to the heuristic value for that field."""
    start, end = text.find("["), text.rfind("]")
    if start < 0 or end < 0:
        return proposals
    try:
        items = json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return proposals
    if not isinstance(items, list):
        return proposals
    by_index = {}
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("index"), int):
            by_index[item["index"]] = item
    for i, p in enumerate(proposals):
        item = by_index.get(i)
        if not item:
            continue
        title = str(item.get("title", "")).strip().replace("—", ",")
        if 3 <= len(title) <= 120:
            p.title = title
        if item.get("area") in AREAS:
            p.area = item["area"]
        if item.get("kind") in KINDS:
            p.kind = item["kind"]
        if item.get("owner") in OWNERS:
            p.owner = item["owner"]
        p.named_by = "llm"
    return proposals


def format_proposals(proposals: list[Proposal]) -> str:
    if not proposals:
        return "no actionable pieces found"
    lines = [f"{len(proposals)} proposed task(s):", ""]
    for i, p in enumerate(proposals):
        lines.append(f"[{i}] {p.area} / {p.kind} / {p.owner} ({p.named_by})")
        lines.append(f"    title: {p.title}")
        lines.append(f"    from:  {p.piece.text[:160]}")
    return "\n".join(lines)
