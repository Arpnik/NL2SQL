from __future__ import annotations

import pytest

from com.nl2sql.guardrails.base import GuardrailContext, GuardrailStatus
from com.nl2sql.guardrails.output_guardrail import OutputGuardrail

DEPT = "Engineering"
SESSION = "session-456"
SENTINEL = "__dept_sentinel__"


def _ctx(dept: str = DEPT) -> GuardrailContext:
    return GuardrailContext(sql="SELECT 1", department=dept, session_id=SESSION)


def _row(name: str, sentinel: str | None = DEPT, dept_col: str | None = None) -> dict:
    """Build a result row. sentinel=None omits the sentinel column entirely."""
    row = {"Name": name, "SalaryAmount": 100_000}
    if sentinel is not None:
        row[SENTINEL] = sentinel
    if dept_col is not None:
        row["Department"] = dept_col
    return row


@pytest.fixture
def guard() -> OutputGuardrail:
    return OutputGuardrail()


# ── validate() must raise ─────────────────────────────────────────────────────

class TestValidateNotImplemented:
    def test_validate_raises(self, guard):
        with pytest.raises(NotImplementedError):
            guard.validate(_ctx())


# ── Empty rows ────────────────────────────────────────────────────────────────

class TestEmptyRows:
    def test_empty_rows_passes(self, guard):
        result, clean = guard.validate_rows(_ctx(), [])
        assert result.status == GuardrailStatus.PASS
        assert clean == []


# ── Should PASS — sentinel present and correct ────────────────────────────────

class TestPassWithSentinel:
    def test_single_correct_row(self, guard):
        rows = [_row("Alice", sentinel=DEPT)]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.PASS
        assert len(clean) == 1

    def test_multiple_correct_rows(self, guard):
        rows = [_row("Alice", sentinel=DEPT), _row("Bob", sentinel=DEPT)]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.PASS
        assert len(clean) == 2

    def test_sentinel_stripped_from_output(self, guard):
        rows = [_row("Alice", sentinel=DEPT)]
        _, clean = guard.validate_rows(_ctx(), rows)
        assert SENTINEL not in clean[0]
        assert "Name" in clean[0]

    def test_other_columns_preserved(self, guard):
        rows = [_row("Alice", sentinel=DEPT)]
        _, clean = guard.validate_rows(_ctx(), rows)
        assert clean[0]["Name"] == "Alice"
        assert clean[0]["SalaryAmount"] == 100_000


# ── Should PASS — no sentinel, Department column correct ─────────────────────

class TestPassWithDeptColumn:
    def test_dept_column_correct_no_sentinel(self, guard):
        rows = [_row("Alice", sentinel=None, dept_col=DEPT)]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.PASS

    def test_no_sentinel_no_dept_column(self, guard):
        # No dept info at all — passes (nothing to scan)
        rows = [{"Name": "Alice", "SalaryAmount": 50_000}]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.PASS
        assert clean[0]["Name"] == "Alice"


# ── Should REJECT — sentinel mismatch ────────────────────────────────────────

class TestRejectSentinelMismatch:
    def test_single_wrong_dept(self, guard):
        rows = [_row("Eve", sentinel="Sales")]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.REJECT
        assert clean == []

    def test_all_rows_wrong_dept(self, guard):
        rows = [_row("Eve", sentinel="Sales"), _row("Mallory", sentinel="Marketing")]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.REJECT
        assert clean == []

    def test_mixed_rows_one_leak(self, guard):
        # One valid row, one leaked row — entire result discarded
        rows = [_row("Alice", sentinel=DEPT), _row("Eve", sentinel="Sales")]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.REJECT
        assert clean == []

    def test_leak_count_in_metadata(self, guard):
        rows = [_row("Eve", sentinel="Sales"), _row("Mallory", sentinel="Marketing")]
        result, _ = guard.validate_rows(_ctx(), rows)
        assert len(result.metadata["leaks"]) == 2

    def test_leak_metadata_content(self, guard):
        rows = [_row("Eve", sentinel="Sales")]
        result, _ = guard.validate_rows(_ctx(), rows)
        leak = result.metadata["leaks"][0]
        assert leak["row_dept"] == "Sales"
        assert leak["expected"] == DEPT

    def test_reason_contains_dept(self, guard):
        rows = [_row("Eve", sentinel="Sales")]
        result, _ = guard.validate_rows(_ctx(), rows)
        assert DEPT in result.reason


# ── Should REJECT — Department column mismatch (belt-and-suspenders) ─────────

class TestRejectDeptColumnMismatch:
    def test_dept_column_wrong_dept(self, guard):
        # No sentinel, but Department column is wrong
        rows = [_row("Eve", sentinel=None, dept_col="Sales")]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.REJECT
        assert clean == []

    def test_sentinel_correct_dept_column_wrong(self, guard):
        # Sentinel says correct dept — sentinel wins, dept column ignored
        rows = [_row("Alice", sentinel=DEPT, dept_col="Sales")]
        result, clean = guard.validate_rows(_ctx(), rows)
        assert result.status == GuardrailStatus.REJECT
        assert len(clean) == 0