"""
Tests for ASTGuardrail — structural SQL policy enforcement.

Policy under test
-----------------
1. BLOCKED TABLES  : employee, certification, benefits — rejected in any scope,
                     regardless of WHERE clause.
2. DEPT-FILTERED VIEWS : dept_employees, dept_certifications, dept_benefits —
                     every SELECT scope touching one of these MUST carry
                     WHERE Department = '<dept>'.

Naming convention
-----------------
  test_<what>_<expected_outcome>
  PASS  → query is compliant
  REJECT → query violates policy
"""

from __future__ import annotations

import pytest

from com.nl2sql.guardrails.ast_guardrail import ASTGuardrail
from com.nl2sql.guardrails.base import GuardrailContext, GuardrailStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEPT = "Engineering"
_SESSION = "sess-test"


def _ctx(sql: str, department: str = _DEPT) -> GuardrailContext:
    return GuardrailContext(
        sql=sql,
        department=department,
        session_id=_SESSION,
    )


@pytest.fixture()
def guard() -> ASTGuardrail:
    return ASTGuardrail()


# ---------------------------------------------------------------------------
# TestBlockedTables
# Direct access to raw sensitive tables must always be rejected.
# ---------------------------------------------------------------------------

class TestBlockedTables:

    def test_raw_employee_no_filter_reject(self, guard):
        """Direct SELECT on employee with no filter."""
        sql = "SELECT Name FROM employee"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("employee" in v for v in r.metadata["violations"])

    def test_raw_employee_with_dept_filter_still_reject(self, guard):
        """Even with the correct dept filter, raw employee is blocked."""
        sql = "SELECT Name FROM employee WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("employee" in v for v in r.metadata["violations"])

    def test_raw_certification_reject(self, guard):
        """Direct SELECT on certification is always blocked."""
        sql = "SELECT CertName FROM certification WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("certification" in v for v in r.metadata["violations"])

    def test_raw_benefits_reject(self, guard):
        """Direct SELECT on benefits is always blocked."""
        sql = "SELECT Plan FROM benefits WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("benefits" in v for v in r.metadata["violations"])

    def test_multiple_blocked_tables_reject(self, guard):
        """Joining two blocked tables produces two violations."""
        sql = """
            SELECT e.Name, b.Plan
            FROM employee e
            JOIN benefits b ON e.EmployeeId = b.EmployeeId
            WHERE e.Department = 'Engineering'
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert len(r.metadata["violations"]) == 2


# ---------------------------------------------------------------------------
# TestDeptFilteredViews — top-level only
# ---------------------------------------------------------------------------

class TestDeptFilteredViews:

    def test_view_correct_filter_pass(self, guard):
        """dept_employees with correct department filter — should pass."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.PASS

    def test_view_no_filter_reject(self, guard):
        """dept_employees with no WHERE clause — reject."""
        sql = "SELECT Name FROM dept_employees"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert r.metadata["violations"]

    def test_view_wrong_department_reject(self, guard):
        """dept_employees filtered on a different department — reject."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Sales'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert r.metadata["violations"]

    def test_view_alias_qualified_filter_pass(self, guard):
        """Alias-qualified predicate (e.Department = '...') is accepted."""
        sql = "SELECT e.Name FROM dept_employees e WHERE e.Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.PASS

    def test_dept_certifications_correct_filter_pass(self, guard):
        sql = "SELECT CertName FROM dept_certifications WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.PASS

    def test_dept_certifications_no_filter_reject(self, guard):
        sql = "SELECT CertName FROM dept_certifications"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT

    def test_dept_benefits_correct_filter_pass(self, guard):
        sql = "SELECT Plan FROM dept_benefits WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.PASS

    def test_dept_benefits_no_filter_reject(self, guard):
        sql = "SELECT Plan FROM dept_benefits"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT


# ---------------------------------------------------------------------------
# TestSubqueryLeaks — subqueries must each carry their own filter / not use blocked tables
# ---------------------------------------------------------------------------

class TestSubqueryLeaks:

    def test_outer_view_filtered_inner_blocked_table_reject(self, guard):
        """
        Outer SELECT on dept_employees (filtered) is fine.
        Inner subquery hits raw employee — must be rejected.
        """
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.Department = 'Engineering'
            AND e.EmployeeId IN (
                SELECT EmployeeId FROM employee
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("employee" in v for v in r.metadata["violations"])

    def test_outer_view_filtered_inner_blocked_with_filter_reject(self, guard):
        """
        Inner subquery on raw employee is rejected even when it has
        a WHERE clause — the table itself is blocked.
        """
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.Department = 'Engineering'
            AND e.EmployeeId IN (
                SELECT EmployeeId FROM employee WHERE Department = 'Engineering'
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("employee" in v for v in r.metadata["violations"])

    def test_outer_view_filtered_inner_view_filtered_pass(self, guard):
        """
        Both outer and inner scopes use approved views with correct filters.
        """
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.Department = 'Engineering'
            AND e.EmployeeId IN (
                SELECT EmployeeId FROM dept_certifications
                WHERE Department = 'Engineering'
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.PASS

    def test_outer_view_filtered_inner_view_missing_filter_reject(self, guard):
        """
        Outer scope is fine, inner subquery on a dept_* view has no filter.
        """
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.Department = 'Engineering'
            AND e.EmployeeId IN (
                SELECT EmployeeId FROM dept_certifications
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("dept_certifications" in v for v in r.metadata["violations"])

    def test_outer_view_filtered_inner_view_wrong_dept_reject(self, guard):
        """
        Inner subquery filters on the wrong department.
        """
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.Department = 'Engineering'
            AND e.EmployeeId IN (
                SELECT EmployeeId FROM dept_certifications
                WHERE Department = 'Sales'
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT

    def test_both_scopes_missing_filter_two_violations(self, guard):
        """
        Outer and inner both reference dept_* views without any filter.
        Expect exactly 2 violations.
        """
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.EmployeeId IN (
                SELECT EmployeeId FROM dept_certifications
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert len(r.metadata["violations"]) == 2

    def test_outer_blocked_inner_view_filtered_reject(self, guard):
        """
        Outer scope hits raw employee — inner being fine doesn't save it.
        """
        sql = """
            SELECT Name FROM employee e
            WHERE e.EmployeeId IN (
                SELECT EmployeeId FROM dept_certifications
                WHERE Department = 'Engineering'
            )
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("employee" in v for v in r.metadata["violations"])


# ---------------------------------------------------------------------------
# TestMultiViewJoins — joins across multiple approved views
# ---------------------------------------------------------------------------

class TestMultiViewJoins:

    def test_join_two_views_both_filtered_pass(self, guard):
        """Joining dept_employees and dept_certifications, both filtered."""
        sql = """
            SELECT e.Name, c.CertName
            FROM dept_employees e
            JOIN dept_certifications c ON e.EmployeeId = c.EmployeeId
            WHERE e.Department = 'Engineering'
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.PASS

    def test_join_two_views_no_filter_reject(self, guard):
        """Joining two approved views but no dept filter anywhere."""
        sql = """
            SELECT e.Name, c.CertName
            FROM dept_employees e
            JOIN dept_certifications c ON e.EmployeeId = c.EmployeeId
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT

    def test_join_approved_and_blocked_reject(self, guard):
        """One approved view joined with a blocked table — must reject."""
        sql = """
            SELECT e.Name, b.Plan
            FROM dept_employees e
            JOIN benefits b ON e.EmployeeId = b.EmployeeId
            WHERE e.Department = 'Engineering'
        """
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.REJECT
        assert any("benefits" in v for v in r.metadata["violations"])


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_empty_sql_reject(self, guard):
        r = guard.validate(_ctx(""))
        assert r.status == GuardrailStatus.REJECT

    def test_non_employee_table_no_filter_pass(self, guard):
        """
        A SELECT on a table that is neither blocked nor a dept_* view
        requires no dept filter (e.g. a lookup / reference table).
        """
        sql = "SELECT Code, Label FROM job_grades"
        r = guard.validate(_ctx(sql))
        assert r.status == GuardrailStatus.PASS

    def test_different_department_context_pass(self, guard):
        """Filter matches the session department (Sales), not Engineering."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Sales'"
        r = guard.validate(_ctx(sql, department="Sales"))
        assert r.status == GuardrailStatus.PASS

    def test_different_department_context_reject(self, guard):
        """Filter is Engineering but session dept is Sales — reject."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql, department="Sales"))
        assert r.status == GuardrailStatus.REJECT

    def test_result_layer_name(self, guard):
        """GuardrailResult.layer should always be 'ASTGuardrail'."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.layer == "ASTGuardrail"

    def test_pass_result_has_no_violations(self, guard):
        """A passing result must carry no violations in metadata."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Engineering'"
        r = guard.validate(_ctx(sql))
        assert r.metadata.get("violations") is None or r.metadata.get("violations") == []