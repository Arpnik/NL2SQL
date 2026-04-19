from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from rich import print

from com.nl2sql.guardrails.base import GuardrailStatus

logger = logging.getLogger(__name__)


@dataclass
class AuditEntry:
    session_id: str
    attempt: int
    layer: str                        # which guardrail produced this entry
    status: str                       # GuardrailStatus.value
    department: str
    sql: str
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


class AuditLogger:
    """
    Appends structured JSON audit entries to a log file.
    One entry per guardrail decision (both PASS and REJECT).
    Thread-safe for single-process use (file append is atomic on most OS).

    Usage:
        audit = AuditLogger(Path("audit.log"))
        audit.log(result, ctx)
    """

    def __init__(self, log_path: Path) -> None:
        self._path = log_path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def log(
        self,
        layer: str,
        status: GuardrailStatus,
        department: str,
        session_id: str,
        attempt: int,
        sql: str,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        entry = AuditEntry(
            session_id=session_id,
            attempt=attempt,
            layer=layer,
            status=status.value,
            department=department,
            sql=sql,
            reason=reason,
            metadata=metadata or {},
        )
        line = entry.to_json()

        try:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError as exc:
            # Audit failure must never crash the app — log to stderr only.
            logger.error("[AuditLogger] Failed to write entry: %s", exc)

        # Mirror blocked requests to console for visibility during demo
        if status == GuardrailStatus.REJECT:
            print(f"[red][AUDIT BLOCK] layer={layer} attempt={attempt} "
                f"dept={department} reason={reason!r} [/red]")