from __future__ import annotations

import logging
import sys

from com.nl2sql.db_session_manager import SessionManager
from com.nl2sql.pipeline import Pipeline
from com.nl2sql.settings import Settings

"""
Entry point — console REPL loop.

Run:
    python console.py

Exit:
    Type 'exit' or 'quit', or press Ctrl+C.
"""

def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def main() -> None:
    try:
        settings = Settings()
    except Exception as exc:
        print(f"[FATAL] Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    _configure_logging(settings.log_level)
    print(settings.display())

    session = SessionManager(settings=settings, department=settings.department)

    try:
        pipeline = Pipeline(session=session, settings=settings)
    except Exception as exc:
        print(f"[FATAL] Pipeline initialisation failed: {exc}", file=sys.stderr)
        session.close()
        sys.exit(1)

    # ── REPL ──────────────────────────────────────────────────────────────────
    print("\nType your question and press Enter. Type 'exit' to quit.\n")

    try:
        while True:
            try:
                question = input("You: ").strip()
            except EOFError:
                break

            if not question:
                continue

            if question.lower() in {"exit", "quit", "q"}:
                break

            result = pipeline.run(question)
            print(result.display())

    except KeyboardInterrupt:
        print("\n[INFO] Interrupted.")

    finally:
        pipeline.shutdown()


if __name__ == "__main__":
    main()