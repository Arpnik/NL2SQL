from __future__ import annotations

from anthropic import Anthropic

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult

INVALID_QUERY_MESSAGE = (
    "Your question doesn't appear to be answerable from the employee database. "
    "The database contains information about employees, their certifications, and benefits. "
    "Please refine your question and try again."
)

_SYSTEM_PROMPT = """You are a query validation assistant for an employee database.

The database contains ONLY:
  - dept_employees  : employee records (name, role, start date, salary, bonus, department)
  - Certification   : employee certifications (name, date achieved)
  - Benefits        : employee benefits packages and remaining balances

Reply with ONLY one token:
  VALID    — the question can be answered from the schema above
  INVALID  — the question is off-topic, incoherent, or references data not in the schema
"""

class QueryValidationGuardrail(BaseGuardrail):
    """
    Layer 0 — pre-generation question validity check.

    Classifies the user question as VALID or INVALID before any SQL is generated.
    INVALID → sets final_error with a fixed user-facing message; graph exits immediately.
    VALID   → PASS; generation proceeds normally.
    Fails open on API errors so transient failures don't block users.
    """

    def __init__(self, model: str = "claude-haiku-4-5-20251001") -> None:
        self._client = Anthropic()
        self._model = model

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        question = ctx.user_question.strip()
        if not question:
            return self._reject("", INVALID_QUERY_MESSAGE, metadata={"reason": "empty_question"})

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=10,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": question}],
            )
            verdict = response.content[0].text.strip().upper()
        except Exception as exc:
            # Fail open — don't block the user on a transient API error
            return self._pass(ctx.sql, metadata={"validation_skipped": str(exc)})

        if verdict != "VALID":
            return self._reject("", INVALID_QUERY_MESSAGE, metadata={"verdict": verdict})

        return self._pass(ctx.sql)