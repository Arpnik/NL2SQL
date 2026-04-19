from __future__ import annotations

import pytest

from com.nl2sql.guardrails.base import GuardrailContext, GuardrailStatus
from com.nl2sql.guardrails.schema_guardrail import SchemaGuardrail

DEPT = "Marketing"
SESSION = "test-session"


def _ctx(sql: str) -> GuardrailContext:
    return GuardrailContext(sql=sql, department=DEPT, session_id=SESSION)


@pytest.fixture
def guard() -> SchemaGuardrail:
    return SchemaGuardrail()


# ── Should PASS ───────────────────────────────────────────────────────────────

class TestPass:
    def test_simple_select(self, guard):
        r = guard.validate(_ctx("SELECT Name FROM dept_employees WHERE "
                                "Department = 'Marketing'"))
        assert r.status == GuardrailStatus.PASS

    def test_join_allowed_views(self, guard):
        sql = """
            SELECT e.Name, c.CertificationName
            FROM dept_employees e
            JOIN dept_certifications c ON c.EmployeeId = e.EmployeeId
            WHERE e.Department = 'Marketing'
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.PASS

    def test_aggregate(self, guard):
        r = guard.validate(_ctx("SELECT SUM(SalaryAmount) FROM dept_employees "
                                "WHERE Department = 'Marketing'"))
        assert r.status == GuardrailStatus.PASS


# ── Should REJECT — forbidden keywords ───────────────────────────────────────

class TestForbiddenKeywords:
    @pytest.mark.parametrize("sql", [
        "DELETE FROM dept_employees WHERE Department = 'Marketing'",
        "DROP TABLE Employee",
        "INSERT INTO Employee VALUES (1, 'x', 'Marketing', 'Eng', '2020-01-01', 1000, 0)",
        "UPDATE Employee SET SalaryAmount = 0",
        "SELECT * FROM sqlite_master",
        "PRAGMA table_info(Employee)",
        "ATTACH DATABASE 'other.db' AS other",
    ])
    def test_forbidden_keyword(self, guard, sql):
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert r.reason is not None


# ── Should REJECT — forbidden statement types ─────────────────────────────────

class TestForbiddenStatements:
    def test_delete_statement(self, guard):
        r = guard.validate(_ctx("DELETE FROM dept_employees"))
        assert r.status == GuardrailStatus.REJECT

    def test_insert_statement(self, guard):
        r = guard.validate(_ctx("INSERT INTO dept_employees (Name) VALUES ('hacker')"))
        assert r.status == GuardrailStatus.REJECT


# ── Should REJECT — UNION / INTERSECT / EXCEPT ───────────────────────────────

class TestSetOperations:
    def test_union_cross_dept(self, guard):
        sql = """
            SELECT Name FROM dept_employees WHERE Department = 'Marketing'
            UNION
            SELECT Name FROM Employee WHERE Department = 'Engineering'
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT

    def test_intersect(self, guard):
        sql = """
            SELECT EmployeeId FROM dept_employees
            INTERSECT
            SELECT EmployeeId FROM Employee
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT


# ── Should REJECT — disallowed tables ────────────────────────────────────────

class TestDisallowedTables:
    def test_raw_system_table(self, guard):
        # sqlite_master caught by keyword scan first, but test table whitelist too
        r = guard.validate(_ctx("SELECT Name FROM SomeOtherTable"))
        assert r.status == GuardrailStatus.REJECT
        assert "disallowed" in r.reason.lower()

    def test_semicolon_injection(self, guard):
        # Two statements — should reject on statement count
        sql = "SELECT Name FROM dept_employees; DELETE FROM Employee"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT