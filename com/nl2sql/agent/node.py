from __future__ import annotations

import logging
import sqlite3
from typing import Any

import anthropic

from com.nl2sql.audit_logger import AuditLogger
from com.nl2sql.guardrails.ast_guardrail import ASTGuardrail
from com.nl2sql.guardrails.base import GuardrailContext, GuardrailStatus
from com.nl2sql.guardrails.output_guardrail import OutputGuardrail
from com.nl2sql.guardrails.prompt_guardrail import PromptGuardrail
from com.nl2sql.guardrails.query_validation_guardrail import (
    INVALID_QUERY_MESSAGE,
    QueryValidationGuardrail,
)
from com.nl2sql.guardrails.schema_guardrail import SchemaGuardrail
from com.nl2sql.guardrails.view_guardrail import ViewGuardrail
from com.nl2sql.models import AgentState

logger = logging.getLogger(__name__)


"""
LangGraph node functions.

Each node function:
  - Receives the full AgentState dict
  - Returns a partial dict of keys to update (LangGraph merges it)
  - Calls the relevant guardrail or executes SQL
  - Delegates all audit logging to AuditLogger

Node functions are intentionally thin — all logic lives in the guardrail classes.
"""

def _make_ctx(state: dict) -> GuardrailContext:
    return GuardrailContext(
        sql=state.get("sql", ""),
        department=state["department"],
        session_id=state["session_id"],
        attempt=state.get("attempt", 1),
    )

def _audit(
    audit: AuditLogger,
    layer: str,
    status: GuardrailStatus,
    state: dict,
    sql: str,
    reason: str | None = None,
    metadata: dict | None = None,
) -> None:
    audit.log(
        layer=layer,
        status=status,
        department=state["department"],
        session_id=state["session_id"],
        attempt=state.get("attempt", 1),
        sql=sql,
        reason=reason,
        metadata=metadata,
    )

def generate_sql_node(
    state: dict,
    prompt_guard: PromptGuardrail,
    audit: AuditLogger,
) -> dict:
    """
    Calls the Anthropic API to generate SQL from the user's question.
    Uses PromptGuardrail to build the system prompt (with dept injection).
    Increments attempt counter on each call.
    """
    attempt = state.get("attempt", 1)
    ctx = _make_ctx(state)

    system_prompt = prompt_guard.build_system_prompt(
        ctx, rejection_reason=state.get("last_rejection_reason", "")
    )

    settings = state["settings"]
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    logger.info(
        "[generate_sql] Attempt %d — calling LLM for: %r",
        attempt, state["user_question"],
    )

    try:
        response = client.messages.create(
            model=settings.llm_model,
            max_tokens=settings.llm_max_tokens,
            temperature=settings.llm_temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": state["user_question"]}],
        )
        sql = response.content[0].text.strip()
    except Exception as exc:
        error_msg = f"LLM call failed: {exc}"
        logger.error("[generate_sql] %s", error_msg)
        return {"final_error": error_msg, "sql": "", "attempt": attempt}

    logger.debug("[generate_sql] Generated SQL:\n%s", sql)
    _audit(audit, "GenerateSQLNode", GuardrailStatus.PASS, state, sql)

    return {
        "sql": sql,
        "attempt": attempt + 1,          # pre-increment for next retry round
        "last_rejection_reason": None,   # clear previous rejection
        "final_error": None,
    }

def schema_guard_node(
    state: dict,
    guardrail: SchemaGuardrail,
    audit: AuditLogger,
) -> dict:
    ctx = _make_ctx(state)
    result = guardrail.validate(ctx)

    _audit(audit, result.layer, result.status, state, result.sql, result.reason, result.metadata)

    if result.rejected:
        return {"last_rejection_reason": result.reason}

    return {"sql": result.sql, "last_rejection_reason": None}

def ast_guard_node(
    state: dict,
    guardrail: ASTGuardrail,
    audit: AuditLogger,
) -> dict:
    ctx = _make_ctx(state)
    result = guardrail.validate(ctx)

    _audit(audit, result.layer, result.status, state, result.sql, result.reason, result.metadata)

    if result.rejected:
        return {"last_rejection_reason": result.reason}

    return {"sql": result.sql, "last_rejection_reason": None}

def view_guard_node(
    state: dict,
    guardrail: ViewGuardrail,
    audit: AuditLogger,
) -> dict:
    ctx = _make_ctx(state)
    result = guardrail.validate(ctx)

    _audit(audit, result.layer, result.status, state, result.sql, result.reason, result.metadata)

    if result.rejected:
        return {"last_rejection_reason": result.reason}

    # MUTATE — carry the rewritten SQL forward
    return {"sql": result.sql, "last_rejection_reason": None}

def execute_sql_node(state: dict, audit: AuditLogger) -> dict:
    """
    Executes the validated (and view-rewritten) SQL against the SQLite connection.
    Connection comes from state — it was opened read-only by SessionManager.
    """
    sql = state["sql"]
    conn: sqlite3.Connection = state["connection"]

    logger.info("[execute_sql] Executing:\n%s", sql)

    try:
        cursor = conn.execute(sql)
        raw_rows = cursor.fetchall()
        # sqlite3.Row supports dict conversion via keys()
        rows = [dict(row) for row in raw_rows]
    except sqlite3.Error as exc:
        error_msg = f"SQL execution error: {exc}"
        logger.error("[execute_sql] %s", error_msg)
        _audit(audit, "ExecuteSQLNode", GuardrailStatus.REJECT, state, sql, error_msg)
        return {"final_error": error_msg, "rows": []}

    logger.info("[execute_sql] Returned %d row(s)", len(rows))
    _audit(audit, "ExecuteSQLNode", GuardrailStatus.PASS, state, sql)
    return {"rows": rows, "final_error": None}

def output_guard_node(
    state: dict,
    guardrail: OutputGuardrail,
    audit: AuditLogger,
) -> dict:
    ctx = _make_ctx(state)
    rows: list[dict[str, Any]] = state.get("rows", [])

    result, clean_rows = guardrail.validate_rows(ctx, rows)

    _audit(audit, result.layer, result.status, state, result.sql, result.reason, result.metadata)

    if result.rejected:
        return {
            "rows": [],
            "final_error": result.reason,
            "last_rejection_reason": result.reason,
        }

    return {"rows": clean_rows, "final_error": None, "last_rejection_reason": None}

def query_validation_node(
    state: AgentState,
    guardrail: QueryValidationGuardrail,
    audit: AuditLogger,
) -> dict:
    """
    Layer 0 node — validates the user question before any SQL generation.
    On INVALID: sets final_error with the fixed user-facing message.
    The graph router sees final_error and exits immediately — no retry.
    """
    ctx = GuardrailContext(
        sql="",
        department=state["department"],
        session_id=state["session_id"],
        attempt=state["attempt"],
        user_question=state["user_question"],
    )
    result = guardrail.validate(ctx)
    audit.log("query_validation", state["session_id"], result)

    if result.status != "PASS":
        return {
            "final_error": INVALID_QUERY_MESSAGE,
            "last_rejection_reason": result.reason,
        }
    return {"last_rejection_reason": None}