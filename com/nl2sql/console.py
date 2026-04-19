from __future__ import annotations

import logging
import sys

from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from rich import print

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
        print(f"[red][FATAL] Configuration error: {exc} [/red]", file=sys.stderr)
        sys.exit(1)

    _configure_logging(settings.log_level)
    print(settings.display())

    session = SessionManager(settings=settings, skip_log_at_startup=False,
                             department=settings.department)

    try:
        pipeline = Pipeline(session=session, settings=settings)
    except Exception as exc:
        print(f"[red][FATAL] Pipeline initialisation failed: {exc} [/red]", file=sys.stderr)
        session.close()
        sys.exit(1)

    # ── REPL ──────────────────────────────────────────────────────────────────
    print("\n[cyan]Type your question and press Enter. Type 'exit' to quit.[/cyan]\n")

    history = FileHistory(".query_history")

    try:
        while True:
            try:
                question = prompt(
                    "You: ",
                    history=history,
                ).strip()
            except EOFError:
                break

            if not question:
                continue

            if question.lower() in {"exit", "quit", "q"}:
                break

            result = pipeline.run(question)
            print(f"[green]{result.display()}[/green]")

    except KeyboardInterrupt:
        print("\n[cyan] Interrupted.[/cyan]")

    finally:
        pipeline.shutdown()


if __name__ == "__main__":
    main()