"""
Integration tests for the full guardrail pipeline:
    SchemaGuardrail → ASTGuardrail → ViewGuardrail

Each guardrail runs in sequence; a REJECT short-circuits the chain.
Mutated SQL is carried forward via a fresh GuardrailContext.
"""
from __future__ import annotations

import pytest

from com.nl2sql.guardrails.ast_guardrail import ASTGuardrail
from com.nl2sql.guardrails.base import GuardrailContext, GuardrailResult, GuardrailStatus
from com.nl2sql.guardrails.schema_guardrail import SchemaGuardrail
from com.nl2sql.guardrails.view_guardrail import ViewGuardrail

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEPT = "Engineering"
SESSION = "sess-pipeline-test"


def ctx(sql: str, dept: str = DEPT) -> GuardrailContext:
    return GuardrailContext(sql=sql, department=dept, session_id=SESSION)


def passed(r: GuardrailResult) -> bool:
    return r.status in (GuardrailStatus.PASS, GuardrailStatus.MUTATE)


def rejected(r: GuardrailResult) -> bool:
    return r.status == GuardrailStatus.REJECT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def schema_g() -> SchemaGuardrail:
    return SchemaGuardrail()


@pytest.fixture()
def ast_g() -> ASTGuardrail:
    return ASTGuardrail()


@pytest.fixture()
def view_g() -> ViewGuardrail:
    return ViewGuardrail()


@pytest.fixture()
def run(schema_g, ast_g, view_g):
    """
    Returns a callable that runs sql through the full pipeline and
    returns the final GuardrailResult.
    """
    def _run(sql: str, dept: str = DEPT) -> GuardrailResult:
        c = ctx(sql, dept)
        result = None
        for g in (schema_g, ast_g, view_g):
            result = g.validate(c)
            if result.rejected:
                return result
            c = c.with_sql(result.sql)   # carry mutated SQL forward
        return result

    return _run


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGuardrailPipeline:

    # ── Passing cases ────────────────────────────────────────────────────────

    def test_clean_query_passes_all_layers(self, run):
        """Approved view, correct dept filter — should clear all three layers."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Engineering'"
        assert passed(run(sql))

    def test_join_two_views_passes_all_layers(self, run):
        """JOIN across two approved views, single dept filter — should pass."""
        sql = """
            SELECT e.Name, c.CertName
            FROM dept_employees e
            JOIN dept_certifications c ON e.EmployeeId = c.EmployeeId
            WHERE e.Department = 'Engineering'
        """
        assert passed(run(sql))

    def test_dept_filter_enforced_across_sessions(self, run):
        """Same query shape, different dept context — each should pass."""
        for dept in ("Engineering", "Sales", "Marketing"):
            sql = f"SELECT Name FROM dept_employees WHERE Department = '{dept}'"
            assert passed(run(sql, dept=dept))

    # ── Blocked at SchemaGuardrail (layer 2) ─────────────────────────────────

    def test_raw_employee_table_blocked_at_schema(self, run):
        """Direct access to raw employee table — SchemaGuardrail rejects first."""
        sql = "SELECT Name FROM employee WHERE Department = 'Engineering'"
        result = run(sql)
        assert rejected(result)
        assert result.layer == "SchemaGuardrail"

    def test_hallucinated_table_blocked_at_schema(self, run):
        """Unknown table not in the approved allowlist."""
        sql = "SELECT Name FROM EmployeeScoped WHERE Department = 'Engineering'"
        result = run(sql)
        assert rejected(result)
        assert result.layer == "SchemaGuardrail"

    def test_write_stmt_blocked_at_schema(self, run):
        """DML is caught by the keyword scan before AST layer runs."""
        sql = "DELETE FROM dept_employees WHERE EmployeeId = 1"
        result = run(sql)
        assert rejected(result)
        assert result.layer == "SchemaGuardrail"

    def test_union_blocked_at_schema(self, run):
        """UNION is blocked by SchemaGuardrail — never reaches ASTGuardrail."""
        sql = (
            "SELECT Name FROM dept_employees WHERE Department = 'Engineering' "
            "UNION SELECT Name FROM dept_employees WHERE Department = 'Sales'"
        )
        result = run(sql)
        assert rejected(result)
        assert result.layer == "SchemaGuardrail"

    # ── Blocked at ASTGuardrail (layer 3) ────────────────────────────────────

    def test_missing_dept_filter_blocked_at_ast(self, run):
        """Approved view, no WHERE clause — passes schema, blocked by AST layer."""
        sql = "SELECT Name FROM dept_employees"
        result = run(sql)
        assert rejected(result)
        assert result.layer == "ASTGuardrail"

    def test_wrong_dept_filter_blocked_at_ast(self, run):
        """Filter present but for a different department than the session."""
        sql = "SELECT Name FROM dept_employees WHERE Department = 'Sales'"
        result = run(sql, dept="Engineering")
        assert rejected(result)
        assert result.layer == "ASTGuardrail"

    def test_subquery_missing_filter_blocked_at_ast(self, run):
        """Outer scope is clean; inner subquery on approved view has no filter."""
        sql = """
            SELECT Name FROM dept_employees e
            WHERE e.Department = 'Engineering'
            AND e.EmployeeId IN (
                SELECT EmployeeId FROM dept_certifications
            )
        """
        result = run(sql)
        assert rejected(result)
        assert result.layer == "ASTGuardrail"