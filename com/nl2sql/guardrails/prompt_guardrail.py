from __future__ import annotations

from com.nl2sql.guardrails.base import BaseGuardrail, GuardrailContext, GuardrailResult

# Few-shot examples — every example SQL has the department filter.
# These are injected into the system prompt so the LLM sees the pattern clearly.
_FEW_SHOT_EXAMPLES = [
    {
        "question": "Who are the software engineers?",
        "sql": (
            "SELECT e.Name, e.Role "
            "FROM dept_employees e "
            "WHERE e.Department = '{dept}' AND e.Role LIKE '%Engineer%'"
        ),
    },
    {
        "question": "What is the average salary?",
        "sql": (
            "SELECT AVG(e.SalaryAmount) AS avg_salary "
            "FROM dept_employees e "
            "WHERE e.Department = '{dept}'"
        ),
    },
    {
        "question": "Which employees have an AWS certification?",
        "sql": (
            "SELECT e.Name, c.CertificationName, c.DateAchieved "
            "FROM dept_employees e "
            "JOIN dept_certifications c ON c.EmployeeId = e.EmployeeId "
            "WHERE e.Department = '{dept}' AND c.CertificationName LIKE '%AWS%'"
        ),
    },
    {
        "question": "Who has the highest remaining benefits balance?",
        "sql": (
            "SELECT e.Name, b.BenefitsPackage, b.RemainingBalance "
            "FROM dept_employees e "
            "JOIN dept_benefits b ON b.EmployeeId = e.EmployeeId "
            "WHERE e.Department = '{dept}' "
            "ORDER BY b.RemainingBalance DESC "
            "LIMIT 1"
        ),
    },
    {
        "question": "List employees who started after 2023 and their certifications",
        "sql": (
            "SELECT e.Name, e.EmploymentStartDate, c.CertificationName "
            "FROM dept_employees e "
            "LEFT JOIN dept_certifications c ON c.EmployeeId = e.EmployeeId "
            "WHERE e.Department = '{dept}' AND e.EmploymentStartDate > '2023-01-01'"
        ),
    },
]

_SCHEMA_BLOCK = """
Database schema (SQLite):

View: dept_employees
  EmployeeId        INTEGER  PRIMARY KEY
  Name              TEXT     NOT NULL
  Department        TEXT     NOT NULL  -- always filtered to the session department
  Role              TEXT     NOT NULL
  EmploymentStartDate TEXT   NOT NULL  (format: YYYY-MM-DD)
  SalaryAmount      REAL     NOT NULL
  YearlyBonusAmount REAL

View: dept_certifications
  CertificationId   INTEGER  PRIMARY KEY
  EmployeeId        INTEGER  FK -> dept_employees(EmployeeId)
  CertificationName TEXT     NOT NULL
  DateAchieved      TEXT     NOT NULL  (format: YYYY-MM-DD)

View: dept_benefits
  BenefitId         INTEGER  PRIMARY KEY
  EmployeeId        INTEGER  FK -> dept_employees(EmployeeId)
  BenefitsPackage   TEXT     NOT NULL
  RemainingBalance  REAL     NOT NULL

View: dept_employees  -- use this instead of Employee directly
  Same columns as Employee, pre-filtered to the session department.
"""


class PromptGuardrail(BaseGuardrail):
    """
    Layer 1 — pre-generation guardrail.

    Builds the system prompt that is passed to the LLM SQL generation node.
    This class does NOT validate generated SQL — it shapes what the LLM receives
    so that department filtering is reinforced before generation begins.

    On retry (attempt > 1) the prompt escalates: it explicitly tells the LLM
    that previous SQL was rejected and why, and demands the filter be present.

    validate() always returns PASS because this layer runs before SQL exists.
    Call build_system_prompt() from the LangGraph generation node.
    """

    def build_system_prompt(self, ctx: GuardrailContext, rejection_reason: str = "",
                            sql_error: str | None = None,
        last_sql: str = "") -> str:
        dept = ctx.department
        examples_block = "\n\n".join(
            f"Q: {ex['question']}\nSQL:\n{ex['sql'].format(dept=dept)}"
            for ex in _FEW_SHOT_EXAMPLES
        )

        retry_block = ""
        if ctx.attempt > 1 and rejection_reason:
            retry_block = (
                f"\n\nWARNING — YOUR PREVIOUS SQL WAS REJECTED.\n"
                f"Reason: {rejection_reason}\n"
            )
            if sql_error and last_sql:
                retry_block += (
                    f"\nThe following SQL caused a runtime error:\n"
                    f"{last_sql}\n"
                    f"SQLite error: {sql_error}\n"
                )
            retry_block += (
                f"You MUST fix this before responding. "
                f"The WHERE e.Department = '{ctx.department}' clause is non-negotiable."
            )

        return f"""You are a precise SQL assistant for a SQLite employee database.

CRITICAL RULE — READ CAREFULLY:
The current session is locked to department: {dept!r}
Every SQL query you generate MUST include:
    WHERE e.Department = '{dept}'
(or use the dept_employees view, which pre-filters to this department)
You MUST NEVER return data from any other department.
You MUST NEVER omit the department filter under any circumstances.
You MUST generate SELECT statements only — no INSERT, UPDATE, DELETE, DROP, or PRAGMA.
You MUST NOT use UNION, INTERSECT, or EXCEPT.
You MUST NOT reference sqlite_master or any system tables.
{_SCHEMA_BLOCK}
Examples (all correctly filtered to department '{dept}'):
{examples_block}{retry_block}

Respond with ONLY the SQL query — no explanation, no markdown, no backticks."""

    def validate(self, ctx: GuardrailContext) -> GuardrailResult:
        # This layer runs before SQL is generated; it always passes.
        # Its work is done via build_system_prompt().
        return self._pass(ctx.sql)