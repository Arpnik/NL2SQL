from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult

# Only these tables/views are permitted in generated SQL.
_ALLOWED_TABLES = frozenset({"employee", "certification", "benefits", "dept_employees"})

# Any of these keywords in the raw SQL is an immediate hard reject.
_FORBIDDEN_KEYWORDS = frozenset({
    "sqlite_master", "sqlite_temp_master", "sqlite_sequence",
    "pragma", "attach", "detach", "drop", "create", "alter",
    "insert", "update", "delete", "replace", "vacuum",
})

# Forbidden statement types (in addition to keyword scanning).
_FORBIDDEN_STMT_TYPES = (
    exp.Insert, exp.Update, exp.Delete, exp.Drop,
    exp.Create, exp.Alter, exp.Command,
)


class SchemaGuardrail(BaseGuardrail):
    """
    Layer 2 — schema boundary enforcement.

    Checks:
    1. The SQL is parseable by sqlglot.
    2. No forbidden keywords appear in the raw SQL text.
    3. Only approved tables/views are referenced.
    4. The statement is a SELECT (no DML/DDL).
    5. No UNION / INTERSECT / EXCEPT constructs.

    Does NOT check for the department WHERE clause — that is Layer 3's job.
    """

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        sql = ctx.sql.strip()

        # ── Raw text scan for forbidden keywords ──────────────────────────────
        lower_sql = sql.lower()
        for kw in _FORBIDDEN_KEYWORDS:
            if kw in lower_sql:
                return self._reject(
                    sql,
                    f"Forbidden keyword detected: '{kw}'",
                    metadata={"forbidden_keyword": kw},
                )

        # ── Parse ─────────────────────────────────────────────────────────────
        try:
            statements = sqlglot.parse(sql, dialect="sqlite")
        except Exception as exc:
            return self._reject(sql, f"SQL failed to parse: {exc}")

        if not statements or len(statements) != 1:
            return self._reject(
                sql,
                f"Expected exactly 1 SQL statement, got {len(statements) if statements else 0}.",
            )

        tree = statements[0]

        # ── Statement type check ──────────────────────────────────────────────
        if isinstance(tree, _FORBIDDEN_STMT_TYPES):
            return self._reject(
                sql,
                f"Only SELECT statements are allowed. Got: {type(tree).__name__}",
            )

        if not isinstance(tree, exp.Select):
            return self._reject(
                sql,
                f"Statement must be a SELECT. Got: {type(tree).__name__}",
            )

        # ── UNION / INTERSECT / EXCEPT ─────────────────────────────────────────
        if tree.find(exp.Union, exp.Intersect, exp.Except):
            return self._reject(sql, "UNION, INTERSECT, and EXCEPT are not allowed.")

        # ── Table reference whitelist ──────────────────────────────────────────
        referenced = {
            tbl.name.lower()
            for tbl in tree.find_all(exp.Table)
            if tbl.name
        }
        disallowed = referenced - _ALLOWED_TABLES
        if disallowed:
            return self._reject(
                sql,
                f"Query references disallowed table(s): {disallowed}. "
                f"Allowed: {_ALLOWED_TABLES}",
                metadata={"disallowed_tables": list(disallowed)},
            )

        return self._pass(sql)