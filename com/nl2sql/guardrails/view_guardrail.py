from __future__ import annotations

import re

import sqlglot

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult

# Sentinel column injected into every query so OutputGuardrail can scan rows.
_SENTINEL_ALIAS = "__dept_sentinel__"

# Regex to rewrite bare "Employee" table references to "dept_employees".
# Uses word boundaries to avoid partial matches (e.g. "EmployeeId" column).
_EMPLOYEE_TABLE_RE = re.compile(r"\bEmployee\b", re.IGNORECASE)


def _inject_sentinel(sql: str) -> str:
    """
    Prepend 'dept_employees.Department AS __dept_sentinel__' into the SELECT list
    so that OutputGuardrail can verify every returned row belongs to the session dept.
    Skips injection if the sentinel is already present (idempotent).
    """
    if _SENTINEL_ALIAS in sql:
        return sql

    # Use a simple text injection rather than AST rewrite to keep it robust.
    # Pattern: SELECT [DISTINCT] → SELECT [DISTINCT] dept_employees.Department AS __dept_sentinel__,
    sentinel_col = f"dept_employees.Department AS {_SENTINEL_ALIAS}"
    return re.sub(
        r"(?i)\bSELECT\b(\s+DISTINCT\s+|\s+)",
        lambda m: f"SELECT{m.group(1)}{sentinel_col}, ",
        sql,
        count=1,
    )


class ViewGuardrail(BaseGuardrail):
    """
    Layer 4 — DB-level enforcement via view rewrite.

    Rewrites all references to the raw Employee table to the dept_employees view.
    The view is pre-filtered at the database level (see DatabaseViewManager),
    so even a query that somehow passes layers 1–3 without a dept filter will
    still only return rows for the session department.

    Also injects a sentinel column (__dept_sentinel__) into the SELECT list
    so OutputGuardrail can scan every result row for department correctness.

    Returns MUTATE so the pipeline carries the rewritten SQL forward.
    """

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        sql = ctx.sql.strip()
        original_sql = sql
        mutations: list[str] = []

        # ── Step 1: rewrite Employee → dept_employees ─────────────────────────
        rewritten = _EMPLOYEE_TABLE_RE.sub("dept_employees", sql)
        if rewritten != sql:
            mutations.append("Rewrote Employee references to dept_employees view")
            sql = rewritten

        # ── Step 2: inject sentinel column ────────────────────────────────────
        sql_with_sentinel = _inject_sentinel(sql)
        if sql_with_sentinel != sql:
            mutations.append(f"Injected sentinel column {_SENTINEL_ALIAS!r}")
            sql = sql_with_sentinel

        # ── Step 3: validate the rewritten SQL still parses ───────────────────
        try:
            sqlglot.parse_one(sql, dialect="sqlite")
        except Exception as exc:
            return self._reject(
                original_sql,
                f"SQL became unparseable after view rewrite: {exc}",
                metadata={"rewritten_sql": sql},
            )

        reason = "; ".join(mutations) if mutations else None
        return self._mutate(sql, reason)