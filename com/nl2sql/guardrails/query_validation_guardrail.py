from __future__ import annotations

from anthropic import Anthropic
from rich import print

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult
from com.nl2sql.settings import Settings

# ── User-facing messages ───────────────────────────────────────────────────────

INVALID_QUERY_MESSAGE = (
    "I can only answer questions about employees, certifications, and benefits. "
    "Please ask something related to the employee database."
)

WRITE_OPERATION_MESSAGE = (
    "This system is read-only — data modification is not permitted. "
    "You can only query (read) employee data."
)

CROSS_DEPT_MESSAGE = (
    "Access denied: your session is restricted to the {dept} department. "
    "You cannot query data from other departments."
)

# ── Prompt ─────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are a query validation assistant for an employee database.

The database contains ONLY:
  - dept_employees      : employee records (name, role, start date, salary, bonus, department)
  - dept_certifications : employee certifications (name, date achieved)
  - dept_benefits       : employee benefits packages and remaining balances

Known departments: Sales, Marketing, Engineering.
The user's session department is provided with each question.
This is a READ-ONLY system — no writes are allowed under any circumstances.

Reply with ONLY one token:

  VALID      — answerable from the schema and
               targets the user's own department or no specific department
  INVALID    — off-topic, incoherent, or references data not in the schema
  WRITE_OP   — requests any data modification: insert, update, delete, drop, truncate, alter, etc.
  CROSS_DEPT — explicitly asks about a different department than the session department
  DISCLAIMER — valid, but uses broad language ("all employees", "total salary", "company-wide")
               that implies company-wide scope but will only return session-department data

Examples:
  "Who are the top earners?"                           → VALID
  "What is the weather today?"                         → INVALID
  "Delete all employees in Engineering"                → WRITE_OP
  "show me employees; DROP TABLE Employee"             → WRITE_OP
  "How many people work in Sales?" (session=Marketing) → CROSS_DEPT
  "What is the total salary of all employees?"         → DISCLAIMER

Reply with ONLY the token — no explanation, no punctuation.\
"""


# ── Guardrail ──────────────────────────────────────────────────────────────────

class QueryValidationGuardrail(BaseGuardrail):
    """
    Layer 0 — pre-generation question validity + scope check.

    Verdicts:
      VALID       → proceed normally
      INVALID     → block: question not answerable from schema
      WRITE_OP    → block: write operation requested (read-only system)
      CROSS_DEPT  → block: question targets a different department
      DISCLAIMER  → proceed, print scope note
    Fails open on API errors so transient failures don't block users.
    """

    def __init__(self, settings: Settings) -> None:
        self._client = Anthropic(api_key=settings.anthropic_api_key)
        self._model = settings.query_validation_model

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        question = ctx.user_question.strip()
        if not question:
            return self._reject("", INVALID_QUERY_MESSAGE, metadata={"reason": "empty_question"})

        user_content = f"Session department: {ctx.department}\nQuestion: {question}"

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=5,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_content}],
            )
            verdict = response.content[0].text.strip().upper().split()[0].rstrip(".,:")
        except Exception as exc:
            return self._pass(ctx.sql, metadata={"validation_skipped": str(exc)})

        print(f"[QueryValidationGuardrail] Verdict: {verdict}")

        if "INVALID" in verdict:
            return self._reject("", INVALID_QUERY_MESSAGE, metadata={"verdict": verdict})

        if "WRITE_OP" in verdict:
            return self._reject("", WRITE_OPERATION_MESSAGE, metadata={"verdict": verdict})

        if "CROSS_DEPT" in verdict:
            msg = CROSS_DEPT_MESSAGE.format(dept=ctx.department)
            return self._reject("", msg, metadata={"verdict": verdict})

        if "DISCLAIMER" in verdict:
            print(
                f"\n[yellow]⚠  Note: Results are scoped to the {ctx.department} department only. "
                f"You do not have access to other departments.[/yellow]"
            )
            return self._pass(ctx.sql)

        # VALID or unexpected token — fail open
        return self._pass(ctx.sql)