"""Desk ontology — extends the ito-cloud-runtime fleet ontology for desk use.

The fleet ontology covers Supplier/Customer/SKU/Ticket/Bug. The desk adds
Obligation, Contract, Counterparty, and Approval as first-class temporal
entities, plus edges for supersession and delivery.
"""
from pydantic import BaseModel, Field


# --- entity types (desk extensions) -----------------------------------------

class Counterparty(BaseModel):
    """A person or entity on the other side of a desk obligation."""
    platform: str | None = Field(None, description="slack | telegram | email")
    handle: str | None = Field(None, description="username or email")


class Obligation(BaseModel):
    """A desk obligation — something owed to or from a counterparty."""
    direction: str | None = Field(None, description="they_owe_us | we_owe_them")
    status: str | None = Field(None, description="open | drafted | sent | closed | closed_superseded")
    obligation_id: int | None = Field(None, description="ledger id")


class Contract(BaseModel):
    """A formal agreement between desk and counterparty."""
    state: str | None = Field(None, description="Reported state: draft | review | sent | delivered | partially_signed | completion_reported | voided. Delivery is not execution; extraction is not signature verification.")
    sha256: str | None = Field(None, description="digest of exact bytes")


class Approval(BaseModel):
    """An operator approval decision on an obligation."""
    decision: str | None = Field(None, description="approve | reject")
    operator: str | None = Field(None, description="who decided")


class Deal(BaseModel):
    """A commercial deal lane with a counterparty."""
    desk_lane: str | None = Field(None, description="canonical deal key, e.g. pluto, ronit")


# Merge with fleet ontology shape (kept compatible for future migration)
ENTITY_TYPES = {
    "Counterparty": Counterparty,
    "Obligation": Obligation,
    "Contract": Contract,
    "Approval": Approval,
    "Deal": Deal,
}


# --- edge types -------------------------------------------------------------

class HasStatus(BaseModel):
    """An obligation has a lifecycle status."""
    status: str | None = None


class SupersededBy(BaseModel):
    """An obligation was superseded by another."""
    reason: str | None = None


class SignedContract(BaseModel):
    """A source reports execution of a contract by all required parties.

    Keep this legacy edge name for graph compatibility. Never extract it for
    sending, delivery, an unsigned draft, a signature request or partial signing.
    This edge remains a report, not authenticated proof of signatures.
    """
    contract_ref: str | None = Field(None, description="Source reference for the reported completion, if explicitly present; never invent a receipt.")


class HasAsk(BaseModel):
    """A deal has an open ask (e.g. node count, specs)."""
    ask_text: str | None = None
    nodes: int | None = None
    gpu_type: str | None = None


class Owes(BaseModel):
    """Direction of obligation."""
    direction: str | None = None


EDGE_TYPES = {
    "HasStatus": HasStatus,
    "SupersededBy": SupersededBy,
    "SignedContract": SignedContract,
    "HasAsk": HasAsk,
    "Owes": Owes,
}


# Which edge types may connect which entity pairs
EDGE_TYPE_MAP = {
    ("Obligation", "Obligation"): ["SupersededBy"],
    ("Obligation", "Entity"): ["HasStatus", "Owes"],
    ("Deal", "Contract"): ["SignedContract"],
    ("Deal", "Entity"): ["HasAsk"],
    ("Entity", "Entity"): [],
}
