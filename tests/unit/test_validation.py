"""Unit tests for the validation framework.

The framework's whole job is to *fail* when something is wrong, so these tests
mostly assert that it does. A validation suite that silently passes is worse
than no validation at all.
"""
from __future__ import annotations

import pytest

from retail_lakehouse.validation import CheckSuite, ValidationFailed


def test_passing_suite_does_not_raise():
    suite = CheckSuite()
    suite.record("something true", True)
    suite.expect_zero("no offending rows", 0)
    suite.expect_equal("counts match", 5, 5)
    suite.raise_if_failed()


def test_failed_check_raises():
    suite = CheckSuite()
    suite.expect_zero("no offending rows", 3)
    with pytest.raises(ValidationFailed):
        suite.raise_if_failed()


def test_all_checks_run_before_raising():
    """One run must report every problem, not stop at the first."""
    suite = CheckSuite()
    suite.expect_zero("first problem", 1)
    suite.expect_equal("second problem", 4, 5)
    suite.record("this one is fine", True)

    assert len(suite.results) == 3
    assert len(suite.failures) == 2

    with pytest.raises(ValidationFailed) as exc:
        suite.raise_if_failed()
    message = str(exc.value)
    assert "first problem" in message
    assert "second problem" in message


def test_failure_message_reports_what_was_found():
    suite = CheckSuite()
    suite.expect_equal("row count matches source", 12, 9)
    with pytest.raises(ValidationFailed) as exc:
        suite.raise_if_failed()
    assert "9" in str(exc.value)
    assert "12" in str(exc.value)


def test_expect_zero_passes_only_on_zero():
    suite = CheckSuite()
    assert suite.expect_zero("zero rows", 0).passed
    assert not suite.expect_zero("one row", 1).passed


def test_skip_does_not_fail_the_suite():
    """A check that cannot run yet (e.g. no previous run to compare) is not a failure."""
    suite = CheckSuite()
    suite.skip("rerun stability", "no previous run recorded yet")
    suite.raise_if_failed()
    assert suite.failures == []


def test_summary_counts_passes():
    suite = CheckSuite(name="demo")
    suite.record("a", True)
    suite.record("b", False)
    assert suite.summary() == "demo: 1/2 checks passed"
