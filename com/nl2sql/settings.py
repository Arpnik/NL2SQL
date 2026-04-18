from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from com.nl2sql.models import Department


def _find_env_file() -> Path | None:
    """
    Walk up from the current working directory looking for a .env file.
    This means the app works whether you run it from the project root,
    a subdirectory, or inside Docker (where .env may be mounted at /app/.env).
    """
    current = Path.cwd()
    for directory in [current, *current.parents]:
        candidate = directory / ".env"
        if candidate.exists():
            return candidate
    return None


# Load .env before pydantic-settings reads env vars, so explicit file values
# are visible to the OS environment and therefore to BaseSettings.
_env_path = _find_env_file()
if _env_path:
    load_dotenv(_env_path, override=False)  # override=False: real env vars win


class Settings(BaseSettings):
    """
    Central configuration for the NL2SQL console app.

    Priority order (highest → lowest):
      1. Real environment variables (e.g. set in Docker / CI)
      2. Values in the .env file
      3. Defaults declared here

    Usage:
        settings = Settings()
        print(settings.anthropic_api_key)
        print(settings.department)          # None → SessionManager picks randomly
    """

    model_config = SettingsConfigDict(
        env_file=str(_env_path) if _env_path else None,
        env_file_encoding="utf-8",
        case_sensitive=False,       # ANTHROPIC_API_KEY and anthropic_api_key both work
        extra="ignore",             # silently ignore unknown keys in .env
    )

    # ── LLM ──────────────────────────────────────────────────────────────────

    anthropic_api_key: str = Field(
        default="",
        description="Anthropic API key. Required at runtime.",
    )

    llm_model: str = Field(
        default="claude-sonnet-4-20250514",
        description="Anthropic model string used for SQL generation.",
    )

    llm_judge_model: str = Field(
        default="claude-sonnet-4-20250514",
        description="Model used for the semantic LLM Judge check (Layer 3.5).",
    )

    llm_max_tokens: int = Field(
        default=1024,
        ge=64,
        le=8192,
        description="Max output tokens per LLM call.",
    )

    llm_temperature: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Temperature for SQL generation. 0 = deterministic.",
    )

    # ── Session / guardrail ───────────────────────────────────────────────────

    department: Department | None = Field(
        default=None,
        description=(
            "Pin the session to a specific department. "
            "Accepted values: Sales, Marketing, Engineering. "
            "Leave unset (or blank) to let SessionManager pick at random."
        ),
    )

    # ── Database ──────────────────────────────────────────────────────────────

    database_path: Path = Field(
        default=Path("employees.db"),
        description="Path to the SQLite database file.",
    )

    database_read_only: bool = Field(
        default=True,
        description="Open the DB connection in read-only mode (recommended).",
    )

    # ── Pipeline behaviour ────────────────────────────────────────────────────

    max_retries: int = Field(
        default=2,
        ge=0,
        le=5,
        description="How many times the self-correction loop may retry a failed query.",
    )

    # ── Audit / logging ───────────────────────────────────────────────────────

    audit_log_path: Path = Field(
        default=Path("audit.log"),
        description="File path for the audit log. Appended to, never overwritten.",
    )

    log_level: str = Field(
        default="INFO",
        description="Logging level: DEBUG | INFO | WARNING | ERROR",
    )

    # ── Validators ────────────────────────────────────────────────────────────

    @field_validator("department", mode="before")
    @classmethod
    def parse_department(cls, value: str | Department | None) -> Department | None:
        """
        Accept department as a case-insensitive string from the .env file
        and coerce it to a Department enum.

        Examples that all resolve to Department.ENGINEERING:
            DEPARTMENT=Engineering
            DEPARTMENT=engineering
            DEPARTMENT=ENGINEERING

        An empty string or missing key is treated as None → random selection.
        """
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None
        if isinstance(value, Department):
            return value
        normalised = value.strip().title()   # "engineering" → "Engineering"
        try:
            return Department(normalised)
        except ValueError:
            valid = ", ".join(d.value for d in Department)
            raise ValueError(
                f"Invalid department '{value}'. Must be one of: {valid}"
            )

    @field_validator("log_level", mode="before")
    @classmethod
    def normalise_log_level(cls, value: str) -> str:
        upper = value.upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if upper not in allowed:
            raise ValueError(f"log_level must be one of {allowed}, got '{value}'")
        return upper

    @model_validator(mode="after")
    def warn_if_no_api_key(self) -> Settings:
        """Emit a clear error early rather than getting a cryptic 401 later."""
        if not self.anthropic_api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY is not set. "
                "Add it to your .env file or export it as an environment variable."
            )
        return self

    @model_validator(mode="after")
    def warn_if_db_missing(self) -> Settings:
        if not self.database_path.exists():
            raise ValueError(
                f"Database file not found: '{self.database_path}'. "
                "Make sure employees.db is present before starting the app."
            )
        return self

    # ── Convenience ───────────────────────────────────────────────────────────

    def display(self) -> str:
        """Pretty-print non-secret config values — useful at startup."""
        key_preview = (
            f"{self.anthropic_api_key[:8]}{'*' * 20}"
            if self.anthropic_api_key else "NOT SET"
        )
        dept_display = self.department.value if self.department else "random"
        return (
            f"\n{'─' * 50}\n"
            f"  Configuration\n"
            f"{'─' * 50}\n"
            f"  API Key      : {key_preview}\n"
            f"  Model        : {self.llm_model}\n"
            f"  Judge Model  : {self.llm_judge_model}\n"
            f"  Department   : {dept_display}\n"
            f"  Database     : {self.database_path}\n"
            f"  Read-only DB : {self.database_read_only}\n"
            f"  Max retries  : {self.max_retries}\n"
            f"  Audit log    : {self.audit_log_path}\n"
            f"  Log level    : {self.log_level}\n"
            f"{'─' * 50}\n"
        )