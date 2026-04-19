from __future__ import annotations

from anthropic import Anthropic
from rich import print

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult
from com.nl2sql.settings import Settings

INVALID_QUERY_MESSAGE = (
    "Your question doesn't appear to be answerable from the employee database. "
    "The database contains information about employees, their certifications, and benefits. "
    "Please refine your question and try again."
)

CROSS_DEPT_MESSAGE = (
    "Access denied: your session is restricted to the {dept} department. "
    "You cannot query data from other departments."
)

_SYSTEM_PROMPT = """You are a query validation assistant for an employee database.

The database contains ONLY:
  - dept_employees  : employee records (name, role, start date, salary, bonus, department)
  - Certification   : employee certifications (name, date achieved)
  - Benefits        : employee benefits packages and remaining balances

The known departments are: Sales, Marketing, Engineering.
The user's session department will be provided in the question context.

Reply with ONLY one of these tokens:
  VALID         — question can be answered from the schema and targets the user's own department 
                  or no specific department
  INVALID       — question is off-topic, incoherent, or references data not in the schema
  CROSS_DEPT    — question explicitly asks about a different department than the session department
  DISCLAIMER    — question is valid but uses broad language (e.g. "all employees", "total salary", 
                  "everyone", "company-wide") that implies company-wide scope but will only return 
                  session-department data
"""


class QueryValidationGuardrail(BaseGuardrail):
    """
    Layer 0 — pre-generation question validity + scope check.

    Verdicts:
      VALID       → proceed normally
      INVALID     → block with user-facing message, no retry
      CROSS_DEPT  → block with access-denied message, no retry
      DISCLAIMER  → proceed, but flag needs_disclaimer=True in result metadata
    Fails open on API errors so transient failures don't block users.
    """

    def __init__(self, settings: Settings) -> None:
        self._client = Anthropic(api_key=settings.anthropic_api_key)
        self._model = settings.query_validation_model

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        question = ctx.user_question.strip()
        if not question:
            return self._reject("", INVALID_QUERY_MESSAGE, metadata={"reason": "empty_question"})

        # Embed session department so the model can detect cross-dept references
        user_content = f"Session department: {ctx.department}\nQuestion: {question}"

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=10,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_content}],
            )
            verdict = response.content[0].text.strip().upper()
        except Exception as exc:
            return self._pass(ctx.sql, metadata={"validation_skipped": str(exc)})

        if "INVALID" in verdict:
            return self._reject("", INVALID_QUERY_MESSAGE, metadata={"verdict": verdict})

        if "CROSS_DEPT" in verdict:
            msg = CROSS_DEPT_MESSAGE.format(dept=ctx.department)
            return self._reject("", msg, metadata={"verdict": verdict})

        if "DISCLAIMER" in verdict:
            print("Disclaimer should be printed !!!!")
            # Pass through but signal that display should add a scope note
            return self._pass(ctx.sql, metadata={"needs_disclaimer": True})

        # VALID or unexpected token — fail open
        return self._pass(ctx.sql)