from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)


# Maps view name → the SQL used to CREATE it.
# Add future views here — the manager will create any that are missing.
_VIEW_DEFINITIONS: dict[str, str] = {
    "dept_employees": (
        "CREATE VIEW IF NOT EXISTS dept_employees AS "
        "SELECT * FROM Employee WHERE Department = ?"
    ),
}


class DatabaseViewManager:
    """
    Ensures required SQLite views exist before the pipeline runs.

    Why a separate class?
      - Views are session-scoped: dept_employees must be filtered to THIS session's
        department. SQLite views don't support parameters, so we drop and recreate
        the view at the start of each session.
      - SessionManager owns the connection; this class borrows it (no ownership).
      - Keeping view lifecycle out of SessionManager follows single-responsibility.

    Usage:
        view_mgr = DatabaseViewManager(connection, department="Engineering")
        view_mgr.ensure_views()   # call once at startup, before any queries
        view_mgr.drop_views()     # call at shutdown (optional — views are session-local)
    """

    def __init__(self, connection: sqlite3.Connection, department: str) -> None:
        self._conn = connection
        self._department = department

    def ensure_views(self) -> None:
        """
        Drop and recreate all managed views filtered to the session department.

        We always DROP+CREATE (not CREATE IF NOT EXISTS) because a prior session
        may have created dept_employees for a different department, and we must
        never inherit a stale filter.
        """
        self._drop_views()
        self._create_views()

    def drop_views(self) -> None:
        """Public alias — call at session shutdown."""
        self._drop_views()

    def verify_views(self) -> dict[str, bool]:
        """
        Returns a dict of {view_name: exists} for diagnostics/testing.
        """
        cursor = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        )
        existing = {row["name"] for row in cursor.fetchall()}
        return {name: name in existing for name in _VIEW_DEFINITIONS}

    # ── Private ───────────────────────────────────────────────────────────────

    def _drop_views(self) -> None:
        for view_name in _VIEW_DEFINITIONS:
            try:
                self._conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                logger.debug("[ViewManager] Dropped view: %s", view_name)
            except sqlite3.Error as exc:
                logger.warning("[ViewManager] Could not drop view %s: %s", view_name, exc)
        self._conn.commit()

    def _create_views(self) -> None:
        for view_name, create_sql in _VIEW_DEFINITIONS.items():
            try:
                # dept_employees is the only parameterised view right now.
                # For non-parameterised views, omit the second argument.
                if "?" in create_sql:
                    self._conn.execute(create_sql, (self._department,))
                else:
                    self._conn.execute(create_sql)

                logger.info(
                    "[ViewManager] Created view '%s' for department '%s'",
                    view_name,
                    self._department,
                )
                print(
                    f"[INFO] View '{view_name}' ready "
                    f"(filtered to department: {self._department})"
                )
            except sqlite3.Error as exc:
                # A missing view will cause query failures — re-raise here.
                raise RuntimeError(
                    f"Failed to create view '{view_name}': {exc}"
                ) from exc

        self._conn.commit()