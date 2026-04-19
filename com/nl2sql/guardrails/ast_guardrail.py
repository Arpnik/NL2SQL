from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult

# ---------------------------------------------------------------------------
# Table policy
# ---------------------------------------------------------------------------

# Raw tables that must NEVER appear in any SELECT scope.
# Add any new sensitive tables here.
_BLOCKED_TABLES = frozenset({
    "employee",
    "certification",
    "benefits",
})

# Views that are permitted — but every scope referencing them must carry
# a WHERE Department = '<dept>' predicate.
_DEPT_FILTERED_VIEWS = frozenset({
    "dept_employees",
    "dept_certifications",
    "dept_benefits",
})


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _direct_tables(select: exp.Select):
    """
    Yield Table nodes that belong *directly* to this SELECT scope.
    Does NOT descend into nested sub-SELECTs — each scope is checked
    independently by _all_select_scopes.
    """
    for node in select.walk():
        if node is not select and isinstance(node, exp.Select):
            # Stop descent — that scope is handled by its own iteration.
            continue
        if isinstance(node, exp.Table) and node.name:
            yield node


def _scope_has_dept_filter(select: exp.Select, department: str) -> bool:
    """
    Return True when the WHERE clause of *this* scope contains a predicate
    of the form:  <col> = '<department>'
    where <col> resolves to 'department' (alias-qualified or bare).

    Accepts both:
        e.Department = 'Engineering'
        Department   = 'Engineering'
    """
    where = select.find(exp.Where)
    if where is None:
        return False

    for eq in where.find_all(exp.EQ):
        left, right = eq.left, eq.right

        col_name = ""
        if isinstance(left, exp.Column):
            col_name = left.name.lower()
        elif isinstance(left, exp.Dot):
            col_name = (
                left.expression.name.lower()
                if hasattr(left.expression, "name")
                else ""
            )

        val = right.this if isinstance(right, exp.Literal) else ""

        if col_name == "department" and val.lower() == department.lower():
            return True

    return False


# ---------------------------------------------------------------------------
# Guardrail
# ---------------------------------------------------------------------------

class ASTGuardrail(BaseGuardrail):
    """
    Layer 3 — structural SQL validation (deterministic, AST-based).

    Policy
    ------
    1. BLOCKED TABLES  — ``employee``, ``certification``, ``benefits`` (and any
       future entries in ``_BLOCKED_TABLES``) must never appear in any SELECT
       scope.  Direct access is rejected regardless of any WHERE clause.

    2. DEPT-FILTERED VIEWS — ``dept_employees``, ``dept_certifications``,
       ``dept_benefits`` are the approved surfaces.  Every SELECT scope that
       references one of these views must carry an explicit
       ``WHERE Department = '<dept>'`` predicate so data cannot leak across
       department boundaries through subqueries or CTEs.

    This layer does NOT mutate SQL — it rejects and lets the retry loop
    regenerate a compliant query.  The rejection reason is written to be
    directly actionable by the LLM on the next attempt.
    """

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        sql = ctx.sql.strip()

        try:
            statements = sqlglot.parse(sql, dialect="sqlite")
        except Exception as exc:
            return self._reject(sql, f"SQL parse error in ASTGuardrail: {exc}")

        tree = next((s for s in statements if s is not None), None)
        if tree is None:
            return self._reject(sql, "No SQL statements found.")
        violations: list[str] = []

        for i, scope in enumerate(tree.find_all(exp.Select)):
            label = "top-level SELECT" if i == 0 else f"sub-SELECT #{i}"
            tables = list(_direct_tables(scope))
            table_names = {t.name.lower() for t in tables}

            # ── Rule 1: blocked raw tables ──────────────────────────────────
            blocked_hits = table_names & _BLOCKED_TABLES
            for tbl in sorted(blocked_hits):
                violations.append(
                    f"{label} directly references the restricted table '{tbl}' — "
                    f"use the approved view instead "
                    f"(e.g. 'dept_{tbl}s' or the appropriate dept_* view)"
                )

            # ── Rule 2: approved views must carry a dept filter ──────────────
            view_hits = table_names & _DEPT_FILTERED_VIEWS
            if view_hits and not _scope_has_dept_filter(scope, ctx.department):
                views_str = ", ".join(sorted(view_hits))
                violations.append(
                    f"{label} references view(s) [{views_str}] but is missing "
                    f"WHERE Department = '{ctx.department}'"
                )

        if violations:
            reason = (
                f"AST policy violated in {len(violations)} scope(s):\n"
                + "\n".join(f"  - {v}" for v in violations)
                + "\n\nRules:\n"
                "  1. Never query employee / certification / benefits directly.\n"
                "  2. Every SELECT referencing a dept_* view must include "
                f"WHERE Department = '{ctx.department}'."
            )
            return self._reject(sql, reason, metadata={"violations": violations})

        return self._pass(sql)