# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml README.md ./
# --no-install-project: only install dependencies, skip building the package itself
RUN uv sync --no-install-project

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    ANTHROPIC_API_KEY=""

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY com/         ./com/
COPY pyproject.toml .
COPY data/employees.db .

CMD ["python", "-m", "com.nl2sql.console"]