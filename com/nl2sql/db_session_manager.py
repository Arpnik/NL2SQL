from __future__ import annotations

import random
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

from rich import print

from com.nl2sql.models import Department, SessionState
from com.nl2sql.settings import Settings


class SessionManager:
    """
    Owns department selection, sqlite session/connection lifecycle,
    and session-scoped counters.

    Responsibilities:
    - Randomly select one of the three departments at startup
    - Create and manage a sqlite connection for the session
    - Log the selection to console
    - Expose the selected department as a read-only property
    - Track per-session query and block counts for audit/reporting
    - Act as the single source of truth — no other class hardcodes the department

    Usage:
        session = SessionManager()          # department selected + logged here
        dept = session.department           # "Engineering"
        session.record_query()
        session.record_blocked_query()
        print(session.summary())
    """

    def __init__(self, settings: Settings, department: Department | None = None) -> None:
        """
        Args:
            department: Pin to a specific department (useful for testing).
                        If None, one is chosen at random — the normal production path.
        """
        self._session_id = str(uuid.uuid4())
        self._started_at = datetime.now()
        self._department = department or random.choice(list(Department))
        self._query_count = 0
        self._blocked_count = 0
        self._settings = settings
        self._connection = self._create_db_connection()

        self._log_startup()

    # ── Public read-only properties ───────────────────────────────────────────

    @property
    def department(self) -> Department:
        """The guardrail department for this session. Set once, never changes."""
        return self._department

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def started_at(self) -> datetime:
        return self._started_at

    @property
    def query_count(self) -> int:
        return self._query_count

    @property
    def blocked_count(self) -> int:
        return self._blocked_count

    # ── State mutation (counters only — department is immutable) ──────────────

    def record_query(self) -> None:
        """Call this every time a query successfully executes."""
        self._query_count += 1

    def record_blocked_query(self) -> None:
        """Call this every time a query is blocked by any guardrail layer."""
        self._blocked_count += 1
        self._query_count += 1  # blocked queries still count as attempted

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> SessionState:
        """
        Returns an immutable point-in-time snapshot of session state.
        Safe to pass around without risk of mutation.
        """
        return SessionState(
            session_id=self._session_id,
            department=self._department,
            started_at=self._started_at,
            query_count=self._query_count,
            blocked_count=self._blocked_count,
        )

    # ── Display helpers ───────────────────────────────────────────────────────

    def summary(self) -> str:
        """Human-readable session summary, used at exit."""
        duration = datetime.now() - self._started_at
        minutes, seconds = divmod(int(duration.total_seconds()), 60)
        successful = self._query_count - self._blocked_count

        return (
            f"\n{'─' * 50}\n"
            f"  Session Summary\n"
            f"{'─' * 50}\n"
            f"  Session ID   : {self._session_id}\n"
            f"  Department   : {self._department.value}\n"
            f"  Duration     : {minutes}m {seconds}s\n"
            f"  Queries      : {self._query_count} total  "
            f"({successful} successful, {self._blocked_count} blocked)\n"
            f"{'─' * 50}\n"
        )

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    def close(self) -> None:
        if self._connection:
            self._connection.close()

    # ── Private ───────────────────────────────────────────────────────────────

    def _log_startup(self) -> None:
        print(f"[yellow][INFO] Session ID    : {self._session_id}[/yellow]")
        print(f"[yellow][INFO] Department selected: {self._department.value}[/yellow]")
        print(f"[yellow][INFO] Started at    : "
              f"{self._started_at.strftime('%Y-%m-%d %H:%M:%S')} [/yellow]")


    def _create_db_connection(self) -> sqlite3.Connection:
        """
        Creates sqlite connection using Settings.

        Read-only:
            file:employees.db?mode=ro

        Read-write:
            employees.db
        """
        db_path: Path = self._settings.database_path

        if self._settings.database_read_only:
            uri = f"file:{db_path}?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
        else:
            conn = sqlite3.connect(str(db_path))

        conn.row_factory = sqlite3.Row
        return conn
