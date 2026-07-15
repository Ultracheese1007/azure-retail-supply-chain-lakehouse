"""A minimal validation framework for the lakehouse.

Design notes:

* **Checks fail loudly.** `CheckSuite.raise_if_failed()` raises
  `ValidationFailed`, so a validation task in a workflow actually fails the run
  instead of printing a warning nobody reads.
* **All checks run before raising.** Failing on the first problem hides the
  others; one run should report everything that is wrong.
* **A check owns its own evidence.** Each result carries the observed value, so
  a failure message says what was expected *and* what was found.

The framework is pure Python and has no Spark dependency, so its behaviour is
unit-tested directly. The Spark queries live in
`databricks/quality/01_validate_pipeline.py`.
"""
from __future__ import annotations

from dataclasses import dataclass, field


class ValidationFailed(AssertionError):
    """Raised when one or more checks fail."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}" + (f" — {self.detail}" if self.detail else "")


@dataclass
class CheckSuite:
    """Collects check results and fails the run if any did not pass."""

    name: str = "lakehouse validation"
    results: list[CheckResult] = field(default_factory=list)

    def record(self, name: str, passed: bool, detail: str = "") -> CheckResult:
        result = CheckResult(name=name, passed=bool(passed), detail=detail)
        self.results.append(result)
        print(str(result))
        return result

    def expect_zero(self, name: str, count: int, detail: str = "") -> CheckResult:
        """Pass when count == 0 (the usual shape: 'how many offending rows?')."""
        message = detail or f"found {count} offending row(s)"
        return self.record(name, count == 0, "" if count == 0 else message)

    def expect_equal(self, name: str, actual, expected, detail: str = "") -> CheckResult:
        message = detail or f"expected {expected!r}, found {actual!r}"
        return self.record(name, actual == expected, "" if actual == expected else message)

    def skip(self, name: str, reason: str) -> CheckResult:
        """Record a check that could not run yet, without failing the suite."""
        result = CheckResult(name=f"{name} (skipped)", passed=True, detail=reason)
        self.results.append(result)
        print(f"[SKIP] {name} — {reason}")
        return result

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    def summary(self) -> str:
        passed = len(self.results) - len(self.failures)
        return f"{self.name}: {passed}/{len(self.results)} checks passed"

    def raise_if_failed(self) -> None:
        print("\n" + self.summary())
        if self.failures:
            lines = "\n".join(f"  - {r.name}: {r.detail}" for r in self.failures)
            raise ValidationFailed(
                f"{len(self.failures)} check(s) failed:\n{lines}"
            )
        print("All checks passed.")
