from __future__ import annotations

import pytest

from com.nl2sql.guardrails.ast_guardrail import ASTGuardrail
from com.nl2sql.guardrails.base import GuardrailContext, GuardrailStatus

DEPT = "Engineering"
SESSION = "session-123"


def _ctx(sql: str, dept: str = DEPT) -> GuardrailContext:
    return GuardrailContext(sql=sql, department=dept, session_id=SESSION)


@pytest.fixture
def guard() -> ASTGuardrail:
    return ASTGuardrail()


# ── Should PASS ───────────────────────────────────────────────────────────────

class TestPass:
    def test_simple_select_with_filter(self, guard):
        sql = "SELECT Name FROM dept_employees e WHERE e.Department = 'Engineering'"
        assert guard.validate(_ctx(sql)).status == GuardrailStatus.PASS

    def test_unaliased_department_column(self, guard):
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Engineering'"
        assert guard.validate(_ctx(sql)).status == GuardrailStatus.PASS

    def test_filter_with_additional_conditions(self, guard):
        sql = """
            SELECT Name, SalaryAmount FROM dept_employees e
            WHERE e.Department = 'Engineering' AND e.SalaryAmount > 100000
        """
        assert guard.validate(_ctx(sql)).status == GuardrailStatus.PASS

    def test_join_certifications_with_filter(self, guard):
        sql = """
            SELECT e.Name, c.CertificationName
            FROM dept_employees e
            JOIN dept_certifications c ON c.EmployeeId = e.EmployeeId
            WHERE e.Department = 'Engineering'
        """
        assert guard.validate(_ctx(sql)).status == GuardrailStatus.PASS

    def test_no_employee_table_no_filter_needed(self, guard):
        # Query only touches dept_certifications — no dept filter required
        sql = "SELECT CertificationName FROM dept_certifications WHERE DateAchieved > '2023-01-01'"
        assert guard.validate(_ctx(sql)).status == GuardrailStatus.PASS

    def test_aggregate_with_filter(self, guard):
        sql = """
            SELECT AVG(SalaryAmount) FROM dept_employees e
            WHERE e.Department = 'Engineering'
        """
        assert guard.validate(_ctx(sql)).status == GuardrailStatus.PASS

    def test_department_case_insensitive(self, guard):
        # department value comparison should be case-insensitive
        sql = "SELECT Name FROM dept_employees WHERE Department = 'engineering'"
        assert guard.validate(_ctx(sql, dept="Engineering")).status == GuardrailStatus.PASS


# ── Should REJECT — missing top-level filter ──────────────────────────────────

class TestMissingTopLevelFilter:
    def test_no_where_clause(self, guard):
        sql = "SELECT Name FROM dept_employees"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert "top-level SELECT" in r.reason

    def test_where_clause_wrong_department(self, guard):
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Sales'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT

    def test_where_clause_wrong_column(self, guard):
        sql = "SELECT Name FROM dept_employees WHERE Role = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT

    def test_filter_on_joined_table_only(self, guard):
        # Filter exists but is on certifications, not dept_employees
        sql = """
            SELECT e.Name FROM dept_employees e
            JOIN dept_certifications c ON c.EmployeeId = e.EmployeeId
            WHERE c.CertificationName = 'AWS'
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT


# ── Should REJECT — subquery leaks ───────────────────────────────────────────

class TestSubqueryLeaks:
    def test_subquery_missing_filter(self, guard):
        # Outer has filter, inner subquery on Employee does not
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.Department = 'Engineering'
            AND e.EmployeeId IN (
                SELECT EmployeeId FROM employee
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert "sub-SELECT" in r.reason

    def test_subquery_wrong_department(self, guard):
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.Department = 'Engineering'
            AND e.EmployeeId IN (
                SELECT EmployeeId FROM employee WHERE Department = 'Sales'
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT

    def test_both_scopes_missing_filter(self, guard):
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.EmployeeId IN (
                SELECT EmployeeId FROM employee
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert r.metadata["violations"]
        assert len(r.metadata["violations"]) == 2   # both scopes flagged


# ── Should REJECT — raw Employee table without filter ─────────────────────────

class TestRawEmployeeTable:
    def test_raw_employee_no_filter(self, guard):
        sql = "SELECT Name FROM employee"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT

    def test_raw_employee_with_filter(self, guard):
        sql = "SELECT Name FROM employee WHERE Department = 'Engineering'"
        assert guard.validate(_ctx(sql)).status == GuardrailStatus.PASS


# ── Metadata ──────────────────────────────────────────────────────────────────

class TestMetadata:
    def test_violations_in_metadata(self, guard):
        sql = "SELECT Name FROM dept_employees"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert "violations" in r.metadata
        assert len(r.metadata["violations"]) == 1

    def test_reason_contains_department(self, guard):
        sql = "SELECT Name FROM dept_employees"
        r = guard.validate(_ctx(sql))
        assert DEPT in r.reason