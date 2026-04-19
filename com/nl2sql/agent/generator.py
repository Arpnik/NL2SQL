from __future__ import annotations

from langgraph.graph import END, StateGraph

from com.nl2sql.agent.node import (
    ast_guard_node,
    execute_sql_node,
    generate_sql_node,
    output_guard_node,
    query_validation_node,
    schema_guard_node,
    view_guard_node,
)
from com.nl2sql.audit_logger import AuditLogger
from com.nl2sql.guardrails.ast_guardrail import ASTGuardrail
from com.nl2sql.guardrails.output_guardrail import OutputGuardrail
from com.nl2sql.guardrails.prompt_guardrail import PromptGuardrail
from com.nl2sql.guardrails.schema_guardrail import SchemaGuardrail
from com.nl2sql.guardrails.view_guardrail import ViewGuardrail
from com.nl2sql.models import AgentState
from com.nl2sql.settings import Settings

"""
LangGraph agent — owns the retry loop and wires nodes together.

Intentionally knows NOTHING about guardrail internals.
It receives GuardrailResult objects and routes on .status only.

Graph shape:
    generate_sql
        ↓
    schema_guard  ──(reject)──┐
        ↓                     │
    ast_guard     ──(reject)──┤
        ↓                     │
    view_guard    ──(reject)──┤
        ↓                     │
    execute_sql               │
        ↓                     │
    output_guard  ──(reject)──┤
        ↓                     │
    END (success)   <─────────┘ (if attempt > max_retries → END with error)
"""

def _route_after_guard(state: AgentState) -> str:
    """
    Called after schema_guard, ast_guard, and view_guard.
    REJECT → back to generate_sql (if retries remain) or END with error.
    PASS / MUTATE → proceed to the next node (caller sets target in add_conditional_edges).
    """
    if state.get("final_error"):
        return "end"
    if state.get("last_rejection_reason"):
        # A guard rejected — attempt is already incremented inside the node.
        if state["attempt"] > state["settings"].max_retries + 1:
            return "end"
        return "generate_sql"
    return "next"


def _route_after_output_guard(state: AgentState) -> str:
    if state.get("final_error") or state.get("last_rejection_reason"):
        return "end"
    return "end"  # success — also ends, caller reads state["rows"]

def _route_after_query_validation(state: AgentState) -> str:
    """INVALID question → end immediately (no retry). Valid → generate SQL."""
    if state.get("final_error"):
        return "end"
    return "generate_sql"


def build_graph(settings: Settings) -> StateGraph:
    """
    Build and compile the LangGraph state machine.
    Guardrail instances are created once and closed over by node functions.
    """
    audit = AuditLogger(settings.audit_log_path)
    prompt_g = PromptGuardrail()
    schema_g = SchemaGuardrail()
    ast_g = ASTGuardrail()
    view_g = ViewGuardrail()
    output_g = OutputGuardrail()

    builder = StateGraph(AgentState)

    # ── Register nodes ────────────────────────────────────────────────────────
    builder.add_node(
        "query_validation",
        lambda s: query_validation_node(s, s["query_validation_guardrail"], audit),
    )
    builder.add_node(
        "generate_sql",
        lambda s: generate_sql_node(s, prompt_g, audit),
    )
    builder.add_node(
        "schema_guard",
        lambda s: schema_guard_node(s, schema_g, audit),
    )
    builder.add_node(
        "ast_guard",
        lambda s: ast_guard_node(s, ast_g, audit),
    )
    builder.add_node(
        "view_guard",
        lambda s: view_guard_node(s, view_g, audit),
    )
    builder.add_node(
        "execute_sql",
        lambda s: execute_sql_node(s, audit),
    )
    builder.add_node(
        "output_guard",
        lambda s: output_guard_node(s, output_g, audit),
    )

    # ── Entry point ───────────────────────────────────────────────────────────
    builder.set_entry_point("query_validation")

    # ── Edges ─────────────────────────────────────────────────────────────────
    builder.add_conditional_edges(
        "query_validation",
        _route_after_query_validation,
        {"generate_sql": "generate_sql", "end": END},
    )

    builder.add_edge("generate_sql", "schema_guard")

    builder.add_conditional_edges(
        "schema_guard",
        _route_after_guard,
        {"next": "ast_guard", "generate_sql": "generate_sql", "end": END},
    )
    builder.add_conditional_edges(
        "ast_guard",
        _route_after_guard,
        {"next": "view_guard", "generate_sql": "generate_sql", "end": END},
    )
    builder.add_conditional_edges(
        "view_guard",
        _route_after_guard,
        {"next": "execute_sql", "generate_sql": "generate_sql", "end": END},
    )

    builder.add_edge("execute_sql", "output_guard")

    builder.add_conditional_edges(
        "output_guard",
        _route_after_output_guard,
        {"end": END},
    )

    return builder.compile()