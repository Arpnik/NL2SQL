from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class Department(StrEnum):
    SALES = "Sales"
    MARKETING = "Marketing"
    ENGINEERING = "Engineering"


@dataclass(frozen=True)
class SessionState:
    """
    Immutable snapshot of the current session.
    frozen=True means no one can accidentally mutate it after creation —
    the department guardrail is set once at startup and never changes.
    """
    session_id: str
    department: Department
    started_at: datetime
    query_count: int = 0
    blocked_count: int = 0


