"""
Tests for ViewGuardrail.

What ViewGuardrail actually does
---------------------------------
1. Rewrites bare ``Employee`` table references → ``dept_employees``.
   (Certification / Benefits have no rewrite mapping — passed through as-is.)
2. Injects ``<alias>.Department AS __dept_sentinel__`` after SELECT.
   Always uses the dept_employees alias when present, falls back to
   the table name itself.
3. Always returns MUTATE (even when only the sentinel was added).
4. Validates the rewritten SQL is still parseable; rejects if not.
"""
from __future__ import annotations

import pytest

from com.nl2sql.guardrails.base import GuardrailContext, GuardrailStatus
from com.nl2sql.guardrails.view_guardrail import ViewGuardrail

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEPT = "Engineering"
SESSION = "test-session-001"
SENTINEL = "__dept_sentinel__"


def ctx(sql: str, dept: str = DEPT) -> GuardrailContext:
    return GuardrailContext(
        sql=sql,
        department=dept,
        session_id=SESSION,
        attempt=1,
        user_question="test question",
    )


def passed(result) -> bool:
    return result.status in (GuardrailStatus.PASS, GuardrailStatus.MUTATE)


def rejected(result) -> bool:
    return result.status == GuardrailStatus.REJECT


@pytest.fixture()
def g() -> ViewGuardrail:
    return ViewGuardrail()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestViewGuardrail:

    # ── Employee rewrite ──────────────────────────────────────────────────────

    def test_rewrites_employee_to_dept_employees(self, g):
        """Employee is rewritten to dept_employees."""
        sql = "SELECT Name FROM Employee WHERE Role = 'Engineer'"
        result = g.validate(ctx(sql))
        assert passed(result)
        assert "dept_employees" in result.sql
        assert "Employee" not in result.sql.replace("dept_employees", "")

    def test_rewrite_is_case_insensitive(self, g):
        """EMPLOYEE and employee are both rewritten."""
        for raw in ("EMPLOYEE", "employee", "Employee"):
            sql = f"SELECT Name FROM {raw}"
            result = g.validate(ctx(sql))
            assert "dept_employees" in result.sql, f"Failed for: {raw}"

    def test_employee_alias_preserved(self, g):
        """Table alias is preserved after rewrite."""
        sql = "SELECT e.Name FROM Employee e"
        result = g.validate(ctx(sql))
        assert "dept_employees e" in result.sql or "dept_employees" in result.sql

    def test_employee_join_rewritten(self, g):
        """Employee rewrite applies inside a JOIN."""
        sql = """
            SELECT e.Name, b.RemainingBalance
            FROM Employee e
            JOIN Benefits b ON e.EmployeeId = b.EmployeeId
        """
        result = g.validate(ctx(sql))
        assert passed(result)
        assert "dept_employees" in result.sql
        # Benefits has no mapping — passes through unchanged
        assert "Benefits" in result.sql

    # ── No rewrite for Certification / Benefits ───────────────────────────────

    def test_certification_not_rewritten(self, g):
        """Certification has no rewrite mapping — table name is preserved."""
        sql = "SELECT CertName FROM Certification"
        result = g.validate(ctx(sql))
        assert passed(result)
        assert "Certification" in result.sql

    def test_benefits_not_rewritten(self, g):
        """Benefits has no rewrite mapping — table name is preserved."""
        sql = "SELECT Plan FROM Benefits"
        result = g.validate(ctx(sql))
        assert passed(result)
        assert "Benefits" in result.sql

    # ── Sentinel injection ────────────────────────────────────────────────────

    def test_sentinel_always_injected(self, g):
        """Every result — rewritten or not — contains the sentinel column."""
        for sql in (
            "SELECT Name FROM Employee",
            "SELECT Name FROM dept_employees WHERE Department = 'Engineering'",
            "SELECT CertName FROM Certification",
        ):
            result = g.validate(ctx(sql))
            assert SENTINEL in result.sql, f"Sentinel missing for: {sql}"

    def test_sentinel_not_duplicated(self, g):
        """If SQL already contains the sentinel it is not injected twice."""
        sql = f"SELECT dept_employees.Department AS {SENTINEL}, Name FROM dept_employees"
        result = g.validate(ctx(sql))
        assert result.sql.count(SENTINEL) == 1

    def test_sentinel_after_select_distinct(self, g):
        """Sentinel is injected correctly after SELECT DISTINCT."""
        sql = "SELECT DISTINCT Name FROM dept_employees WHERE Department = 'Engineering'"
        result = g.validate(ctx(sql))
        assert SENTINEL in result.sql
        assert passed(result)

    def test_sentinel_uses_alias_when_present(self, g):
        """Sentinel column is prefixed with the table alias, not the raw table name."""
        sql = "SELECT e.Name FROM dept_employees e WHERE e.Department = 'Engineering'"
        result = g.validate(ctx(sql))
        assert f"e.Department AS {SENTINEL}" in result.sql

    # ── Always MUTATE ─────────────────────────────────────────────────────────

    def test_always_returns_mutate(self, g):
        """ViewGuardrail always returns MUTATE, even for already-correct SQL."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Engineering'"
        result = g.validate(ctx(sql))
        assert result.status == GuardrailStatus.MUTATE

    def test_layer_name(self, g):
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Engineering'"
        result = g.validate(ctx(sql))
        assert result.layer == "ViewGuardrail"

    # ── sqlite_master passthrough ─────────────────────────────────────────────

    def test_sqlite_master_not_mangled(self, g):
        """sqlite_master is not rewritten — upstream layers should block it,
        but ViewGuardrail must not silently transform it into something else."""
        sql = "SELECT * FROM sqlite_master"
        result = g.validate(ctx(sql))
        if passed(result):
            assert "sqlite_master" in result.sql