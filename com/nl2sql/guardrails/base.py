from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class GuardrailStatus(Enum):
    PASS = "pass"
    REJECT = "reject"
    MUTATE = "mutate"  # SQL was rewritten but approved


@dataclass(frozen=True)
class GuardrailContext:
    """
    Immutable snapshot passed through every guardrail in the pipeline.
    The 'sql' field is replaced (new context created) when a layer mutates the SQL.
    """
    sql: str
    department: str       # session-locked, sourced from SessionManager
    session_id: str
    attempt: int = 1      # incremented by the retry mechanism on each loop

    def with_sql(self, new_sql: str) -> GuardrailContext:
        """Return a new context with updated SQL — preserves all other fields."""
        return GuardrailContext(
            sql=new_sql,
            department=self.department,
            session_id=self.session_id,
            attempt=self.attempt,
        )


@dataclass(frozen=True)
class GuardrailResult:
    """
    Returned by every guardrail.validate() call.

    status  : PASS (proceed), REJECT (block + retry), MUTATE (SQL was rewritten)
    sql     : the SQL to carry forward (may differ from ctx.sql on MUTATE)
    reason  : human-readable explanation — always set on REJECT, optional on MUTATE
    layer   : name of the guardrail that produced this result (set automatically)
    metadata: arbitrary extra data (e.g. which rows failed OutputGuardrail scan)
    """
    status: GuardrailStatus
    sql: str
    layer: str = ""
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status in (GuardrailStatus.PASS, GuardrailStatus.MUTATE)

    @property
    def rejected(self) -> bool:
        return self.status == GuardrailStatus.REJECT


class BaseGuardrail(ABC):
    """
    All guardrails inherit from this. Subclasses implement validate().
    The layer name is derived from the class name automatically.
    """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def _result(
        self,
        status: GuardrailStatus,
        sql: str,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> GuardrailResult:
        return GuardrailResult(
            status=status,
            sql=sql,
            layer=self.name,
            reason=reason,
            metadata=metadata or {},
        )

    def _pass(self, sql: str) -> GuardrailResult:
        return self._result(GuardrailStatus.PASS, sql)

    def _mutate(self, sql: str, reason: str | None = None) -> GuardrailResult:
        return self._result(GuardrailStatus.MUTATE, sql, reason)

    def _reject(self, sql: str, reason: str, metadata: dict | None = None) -> GuardrailResult:
        return self._result(GuardrailStatus.REJECT, sql, reason, metadata)

    @abstractmethod
    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        """
        Inspect ctx.sql and return a GuardrailResult.
        Do NOT raise exceptions for policy violations — return REJECT instead.
        Raise only for unexpected infrastructure errors.
        """
        ...