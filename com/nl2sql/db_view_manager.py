from __future__ import annotations

import logging
import re
import sqlite3

logger = logging.getLogger(__name__)


def _make_dept_employees_sql(department: str) -> str:
    """
    Build the CREATE VIEW SQL for dept_employees, embedding the department
    value directly — SQLite views don't support bound parameters.

    The department name is validated against a strict allowlist pattern
    before interpolation to prevent SQL injection.
    """
    if not re.fullmatch(r"[A-Za-z0-9 _-]{1,64}", department):
        raise ValueError(
            f"Department name contains invalid characters: {department!r}"
        )
    # Use single-quoted string literal; escape any embedded single quotes.
    safe = department.replace("'", "''")
    return (
        f"CREATE VIEW IF NOT EXISTS dept_employees AS "
        f"SELECT * FROM Employee WHERE Department = '{safe}'"
    )


# Maps view name → a callable(department) -> SQL  *or*  a plain SQL string.
# Plain strings are used for views that need no runtime values.
_VIEW_DEFINITIONS: dict[str, str | callable] = {
    "dept_employees": _make_dept_employees_sql,
}


class DatabaseViewManager:
    """
    Ensures required SQLite views exist before the pipeline runs.

    Why a separate class?
      - Views are session-scoped: dept_employees must be filtered to THIS
        session's department. SQLite views don't support parameters, so we
        drop and recreate the view at the start of each session.
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

        Always DROP+CREATE (not CREATE IF NOT EXISTS) because a prior session
        may have created dept_employees for a different department.
        """
        self._drop_views()
        self._create_views()

    def drop_views(self) -> None:
        """Public alias — call at session shutdown."""
        self._drop_views()

    def verify_views(self) -> dict[str, bool]:
        """Returns {view_name: exists} for diagnostics/testing."""
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
                logger.warning(
                    "[ViewManager] Could not drop view %s: %s", view_name, exc
                )
        self._conn.commit()

    def _create_views(self) -> None:
        for view_name, definition in _VIEW_DEFINITIONS.items():
            # Resolve the SQL: call the factory if it's a callable,
            # otherwise use the string directly.
            create_sql = (
                definition(self._department)
                if callable(definition)
                else definition
            )
            try:
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
                raise RuntimeError(
                    f"Failed to create view '{view_name}': {exc}"
                ) from exc

        self._conn.commit()