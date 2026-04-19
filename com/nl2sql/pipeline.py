from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from com.nl2sql.agent.generator import AgentState, build_graph
from com.nl2sql.audit_logger import AuditLogger
from com.nl2sql.db_session_manager import SessionManager
from com.nl2sql.db_view_manager import DatabaseViewManager
from com.nl2sql.guardrails.ast_guardrail import ASTGuardrail
from com.nl2sql.guardrails.output_guardrail import OutputGuardrail
from com.nl2sql.guardrails.prompt_guardrail import PromptGuardrail
from com.nl2sql.guardrails.query_validation_guardrail import QueryValidationGuardrail
from com.nl2sql.guardrails.schema_guardrail import SchemaGuardrail
from com.nl2sql.guardrails.view_guardrail import ViewGuardrail
from com.nl2sql.settings import Settings

logger = logging.getLogger(__name__)


"""
Pipeline — the single public entry point for running a user query.

Responsibilities:
  - Accept a user question
  - Build the initial AgentState and invoke the compiled LangGraph
  - Format and return results
  - Notify SessionManager of success/block

This class is intentionally thin — it wires SessionManager, DatabaseViewManager,
and the LangGraph together, but owns no logic of its own.

Usage:
    pipeline = Pipeline(session, settings)
    result = pipeline.run("Who are the engineers?")
    print(result.display())
"""

@dataclass
class QueryResult:
    question: str
    sql: str
    rows: list[dict[str, Any]]
    error: str | None
    attempt_count: int
    department: str
    needs_disclaimer: bool = False

    @property
    def success(self) -> bool:
        return self.error is None

    def display(self) -> str:
        """Pretty-print the result for the console."""
        lines = [f"\n[SQL]\n{self.sql}\n"]

        if self.error:
            lines.append(f"[ERROR] {self.error}")
            return "\n".join(lines)

        if not self.rows:
            lines.append("[RESULT] No rows returned.")
        else:
            # Build a simple aligned table
            headers = list(self.rows[0].keys())
            col_widths = {h: len(h) for h in headers}
            for row in self.rows:
                for h in headers:
                    col_widths[h] = max(col_widths[h], len(str(row.get(h, ""))))

            sep = "  ".join("-" * col_widths[h] for h in headers)
            header_row = "  ".join(h.ljust(col_widths[h]) for h in headers)

            lines.append("[RESULT]")
            lines.append(header_row)
            lines.append(sep)
            for row in self.rows:
                lines.append(
                    "  ".join(str(row.get(h, "")).ljust(col_widths[h]) for h in headers)
                )
            lines.append(f"\n{len(self.rows)} row(s) | department: {self.department}")

        if self.needs_disclaimer:                # ← add
            lines.append(
                f"\n⚠  Note: Results are scoped to the {self.department} department only. "
                f"You do not have access to other departments."
            )

        return "\n".join(lines)



class Pipeline:
    """
    Orchestrates a single query through the full guardrail + LangGraph pipeline.

    Call pipeline.run(question) for each user turn.
    The compiled graph is built once and reused across calls.
    """

    def __init__(self, session: SessionManager, settings: Settings) -> None:
        self._session = session
        self._settings = settings
        self._audit = AuditLogger(settings.audit_log_path)

        # Compile graph once — reused for every query in the session
        self._graph = build_graph(settings)
        self._query_validation_guardrail = QueryValidationGuardrail(settings)

        # Ensure DB views are ready before any query runs
        # Create a separate write session since we need to generate the views before read session
        write_settings = Settings()
        write_settings.database_read_only = False
        self.write_session = SessionManager(write_settings)
        self._view_manager = DatabaseViewManager(
            connection=self.write_session.connection,
            department=session.department.value,
        )
        self._view_manager.ensure_views()
        # Verify views were created correctly
        status = self._view_manager.verify_views()
        for view, exists in status.items():
            if not exists:
                raise RuntimeError(
                    f"View '{view}' was not created successfully. "
                    "Check DatabaseViewManager logs."
                )
            logger.info("[Pipeline] View verified: %s = %s", view, exists)


    def run(self, question: str) -> QueryResult:
        """
        Execute a single natural-language question through the full pipeline.
        Updates SessionManager counters on completion.
        """
        dept = self._session.department.value

        initial_state: AgentState = {
            "user_question": question,
            "department": dept,
            "session_id": self._session.session_id,
            "connection": self._session.connection,
            "sql": "",
            "attempt": 1,
            "rows": [],
            "last_rejection_reason": None,
            "final_error": None,
            "settings": self._settings,
            "audit_logger": self._audit,
            # Guardrail instances — created fresh per run so state is clean
            "query_validation_guardrail": self._query_validation_guardrail,
            "prompt_guardrail": PromptGuardrail(),
            "schema_guardrail": SchemaGuardrail(),
            "ast_guardrail": ASTGuardrail(),
            "view_guardrail": ViewGuardrail(),
            "output_guardrail": OutputGuardrail(),
            "needs_disclaimer": False,
        }

        logger.info(
            "[Pipeline] Running query (dept=%s): %r", dept, question
        )

        final_state: AgentState = self._graph.invoke(initial_state)

        error = final_state.get("final_error")
        rows = final_state.get("rows", [])
        sql = final_state.get("sql", "")
        attempt_count = final_state.get("attempt", 1) - 1

        result = QueryResult(
            question=question,
            sql=sql,
            rows=rows,
            error=error,
            attempt_count=attempt_count,
            department=dept,
            needs_disclaimer=final_state.get("needs_disclaimer", False),
        )

        if result.success:
            self._session.record_query()
        else:
            self._session.record_blocked_query()

        return result

    def shutdown(self) -> None:
        """Call at application exit to clean up views and close resources."""
        self._view_manager.drop_views()
        self._session.close()
        self.write_session.close()
        print(self._session.summary())