from __future__ import annotations

import re

import sqlglot
import sqlglot.expressions as exp

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult

_SENTINEL_ALIAS = "__dept_sentinel__"
_EMPLOYEE_TABLE_RE = re.compile(r"\bEmployee\b", re.IGNORECASE)
_DEPT_TABLES = frozenset({"dept_employees", "employee"})


def _get_dept_table_alias(sql: str) -> str:
    try:
        tree = sqlglot.parse_one(sql, dialect="sqlite")
        for tbl in tree.find_all(exp.Table):  # ← now resolves correctly
            if tbl.name.lower() in _DEPT_TABLES:
                alias = tbl.alias
                return alias if alias else tbl.name
    except Exception:
        pass
    return "dept_employees"


def _inject_sentinel(sql: str) -> str:
    if _SENTINEL_ALIAS in sql:
        return sql

    alias = _get_dept_table_alias(sql)
    sentinel_col = f"{alias}.Department AS {_SENTINEL_ALIAS}"

    return re.sub(
        r"(?i)\bSELECT\b(\s+DISTINCT\s+|\s+)",
        lambda m: f"SELECT{m.group(1)}{sentinel_col}, ",
        sql,
        count=1,
    )


class ViewGuardrail(BaseGuardrail):
    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        sql = ctx.sql.strip()
        original_sql = sql
        mutations: list[str] = []

        rewritten = _EMPLOYEE_TABLE_RE.sub("dept_employees", sql)
        if rewritten != sql:
            mutations.append("Rewrote Employee references to dept_employees view")
            sql = rewritten

        sql_with_sentinel = _inject_sentinel(sql)
        if sql_with_sentinel != sql:
            mutations.append(f"Injected sentinel column {_SENTINEL_ALIAS!r}")
            sql = sql_with_sentinel

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