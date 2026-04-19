from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict
import sqlite3

from com.nl2sql.audit_logger import AuditLogger
from com.nl2sql.guardrails.ast_guardrail import ASTGuardrail
from com.nl2sql.guardrails.output_guardrail import OutputGuardrail
from com.nl2sql.guardrails.prompt_guardrail import PromptGuardrail
from com.nl2sql.guardrails.query_validation_guardrail import QueryValidationGuardrail
from com.nl2sql.guardrails.schema_guardrail import SchemaGuardrail
from com.nl2sql.guardrails.view_guardrail import ViewGuardrail
from com.nl2sql.settings import Settings
from com.nl2sql.types import Department


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

class AgentState(TypedDict):
    # Inputs (set once at graph entry)
    user_question: str
    department: str
    session_id: str
    connection: sqlite3.Connection

    # Mutable state updated by nodes
    sql: str
    attempt: int
    rows: list[dict[str, Any]]

    # Error tracking
    last_rejection_reason: str | None
    final_error: str | None             # set when max_retries exhausted

    # Injected dependencies (set once at graph entry)
    settings: Settings
    audit_logger: AuditLogger
    prompt_guardrail: PromptGuardrail
    schema_guardrail: SchemaGuardrail
    ast_guardrail: ASTGuardrail
    view_guardrail: ViewGuardrail
    output_guardrail: OutputGuardrail
    query_validation_guardrail: QueryValidationGuardrail

