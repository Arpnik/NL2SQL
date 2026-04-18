from __future__ import annotations

from typing import Iterator

import sqlglot
import sqlglot.expressions as exp

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult

# Every SELECT scope that touches the Employee table must have this filter.
_EMPLOYEE_TABLES = frozenset({"employee", "dept_employees"})


def _all_select_scopes(tree: exp.Expression) -> Iterator[exp.Select]:
    """Yield the top-level SELECT and every sub-SELECT in the AST."""
    yield from tree.find_all(exp.Select)


def _scope_touches_employee(select: exp.Select) -> bool:
    """Return True if this SELECT scope references Employee or dept_employees."""
    return any(
        tbl.name.lower() in _EMPLOYEE_TABLES
        for tbl in select.find_all(exp.Table)
        if tbl.name
    )


def _scope_has_dept_filter(select: exp.Select, department: str) -> bool:
    """
    Return True if the WHERE clause of this SELECT scope contains
    a predicate of the form: <col> = '<department>'
    where <col> is 'department' (with any optional table alias prefix).

    Accepts both:
        e.Department = 'Engineering'
        Department = 'Engineering'
    """
    where = select.find(exp.Where)
    if where is None:
        return False

    for eq in where.find_all(exp.EQ):
        left, right = eq.left, eq.right

        # Normalise column name (strip alias prefix)
        col_name = ""
        if isinstance(left, exp.Column):
            col_name = left.name.lower()
        elif isinstance(left, exp.Dot):
            col_name = left.expression.name.lower() if hasattr(left.expression, "name") else ""

        val = ""
        if isinstance(right, exp.Literal):
            val = right.this

        if col_name == "department" and val.lower() == department.lower():
            return True

    return False


class ASTGuardrail(BaseGuardrail):
    """
    Layer 3 — structural SQL validation (deterministic, code-based).

    Checks that EVERY SELECT scope touching the Employee table has an explicit
    WHERE Department = '<dept>' predicate. This catches:
      - Missing top-level filter
      - Subqueries that join back to Employee without a filter
      - Correlated subqueries that leak cross-dept aggregates

    This layer does NOT mutate SQL — it rejects and lets the retry loop fix it.
    The rejection reason is specific so the LLM knows exactly what to correct.
    """

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        sql = ctx.sql.strip()

        try:
            statements = sqlglot.parse(sql, dialect="sqlite")
        except Exception as exc:
            return self._reject(sql, f"SQL parse error in ASTGuardrail: {exc}")

        if not statements:
            return self._reject(sql, "No SQL statements found.")

        tree = statements[0]
        violations: list[str] = []

        for i, scope in enumerate(_all_select_scopes(tree)):
            if not _scope_touches_employee(scope):
                continue  # This scope doesn't touch Employee — skip

            if not _scope_has_dept_filter(scope, ctx.department):
                label = "top-level SELECT" if i == 0 else f"sub-SELECT #{i}"
                violations.append(
                    f"{label} references the Employee table but is missing "
                    f"WHERE Department = '{ctx.department}'"
                )

        if violations:
            reason = (
                f"Department filter missing in {len(violations)} scope(s):\n"
                + "\n".join(f"  - {v}" for v in violations)
                + f"\nEvery SELECT touching Employee MUST include: "
                f"WHERE e.Department = '{ctx.department}'"
            )
            return self._reject(sql, reason, metadata={"violations": violations})

        return self._pass(sql)