from __future__ import annotations

from typing import Any

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult

_SENTINEL = "__dept_sentinel__"


class OutputGuardrail(BaseGuardrail):
    """
    Layer 5 — post-execution row scanning (last line of defence).

    After the SQL is executed, this guardrail receives the raw result rows.
    It:
      1. Checks every row's __dept_sentinel__ column (injected by ViewGuardrail)
         and rejects if any row belongs to the wrong department.
      2. Strips the sentinel column from every row before the result reaches the user.
      3. Optionally checks the 'Department' column directly if present (belt-and-suspenders).

    This runs AFTER execution, so validate() is overloaded to accept rows.
    The base validate(ctx) raises NotImplementedError — callers must use
    validate_rows(ctx, rows) instead.
    """

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        raise NotImplementedError(
            "OutputGuardrail requires rows. Call validate_rows(ctx, rows) instead."
        )

    def validate_rows(
        self,
        ctx: GuardrailContext,
        rows: list[dict[str, Any]],
    ) -> tuple[GuardrailResult, list[dict[str, Any]]]:
        """
        Validate result rows and return (result, clean_rows).

        clean_rows has the sentinel column stripped and is empty on REJECT.
        """
        if not rows:
            # No rows — nothing to scan; pass through.
            return self._pass(ctx.sql), []

        leaks: list[dict[str, Any]] = []

        for row in rows:
            # Check sentinel column (preferred — always present after ViewGuardrail)
            sentinel_val = row.get(_SENTINEL)
            if sentinel_val is not None and sentinel_val != ctx.department:
                leaks.append({"row_dept": sentinel_val, "expected": ctx.department})
                continue

            # Belt-and-suspenders: check raw Department column if present
            dept_val = row.get("Department")
            if dept_val is not None and dept_val != ctx.department:
                leaks.append({"row_dept": dept_val, "expected": ctx.department})

        if leaks:
            reason = (
                f"Data leakage detected: {len(leaks)} row(s) from a department "
                f"other than '{ctx.department}'. Results discarded."
            )
            return (
                self._reject(ctx.sql, reason, metadata={"leaks": leaks}),
                [],
            )

        # Strip sentinel column before returning to user
        clean_rows = [
            {k: v for k, v in row.items() if k != _SENTINEL}
            for row in rows
        ]
        return self._pass(ctx.sql), clean_rows