"""Regression tests for the demo/Azure split and honesty of claims.

These pin the boundary the two workflows must keep: the demo workflow generates
data and is evidence-backed; the Azure workflow reads ADLS Parquet and is
statically defined. They also guard the README against overclaiming a live Azure
deployment or a Bicep compilation that has not actually run.
"""
from __future__ import annotations

import json
import pathlib

REPO_ROOT = pathlib.Path(__file__).parents[2]
WORKFLOWS = REPO_ROOT / "databricks" / "workflows"
DEMO = WORKFLOWS / "lakehouse_pipeline.json"
AZURE = WORKFLOWS / "azure_lakehouse_pipeline.json"
README = REPO_ROOT / "README.md"
ADLS_LOADER = REPO_ROOT / "databricks" / "bronze" / "03_load_bronze_from_adls.py"


def _task_keys(path: pathlib.Path) -> set[str]:
    data = json.loads(path.read_text())
    return {t["task_key"] for t in data["tasks"]}


def _notebook_paths(path: pathlib.Path) -> set[str]:
    data = json.loads(path.read_text())
    return {t["notebook_task"]["notebook_path"] for t in data["tasks"]}


def test_both_workflows_are_valid_json():
    json.loads(DEMO.read_text())
    json.loads(AZURE.read_text())


def test_demo_workflow_still_generates_source_data():
    assert "generate_source_data" in _task_keys(DEMO)


def test_azure_workflow_does_not_generate_source_data():
    assert "generate_source_data" not in _task_keys(AZURE)


def test_azure_workflow_loads_bronze_from_adls():
    paths = _notebook_paths(AZURE)
    assert "databricks/bronze/03_load_bronze_from_adls" in paths
    assert "databricks/bronze/02_load_bronze" not in paths


def test_azure_workflow_uses_generic_validation():
    paths = _notebook_paths(AZURE)
    assert "databricks/quality/02_validate_generic" in paths
    assert "databricks/quality/01_validate_pipeline" not in paths


def test_demo_workflow_uses_the_demo_specific_validation():
    paths = _notebook_paths(DEMO)
    assert "databricks/quality/01_validate_pipeline" in paths


def test_adls_loader_expects_parquet_load_date_partitions():
    text = ADLS_LOADER.read_text()
    assert "load_date=" in text, "ADLS loader must read load_date partitions"
    assert "read.parquet" in text, "ADLS loader must read Parquet, not CSV"


def test_adls_loader_selects_latest_partition_for_full_loads():
    text = ADLS_LOADER.read_text()
    # incremental -> all partitions; full -> latest only
    assert "all_dates if load_type == \"incremental\" else all_dates[-1:]" in text


def test_generic_validation_has_no_demo_specific_values():
    """The Azure validation must not hard-code planted IDs or exact demo counts."""
    text = (REPO_ROOT / "databricks" / "quality" / "02_validate_generic.py").read_text()
    demo_tokens = (
        "C001", "C002", "C006", "OI2001", "OI2002", "EDGE_CASES", "generate_source_data",
    )
    for demo_token in demo_tokens:
        assert demo_token not in text, f"generic validation leaks demo token {demo_token!r}"


def test_readme_does_not_claim_a_live_azure_deployment():
    text = README.read_text().lower()
    forbidden = [
        "deployed to azure",
        "production deployment",
        "production-grade",
        "live azure data factory",
        "live adls",
        "executed on azure databricks",
        "executed end to end on azure",
    ]
    for phrase in forbidden:
        assert phrase not in text, f"README makes an unsupported claim: {phrase!r}"


def test_readme_bicep_claim_stays_within_what_ci_proves():
    """CI now compiles Bicep, so 'compiled in CI' is accurate. What must never
    appear is a claim beyond compilation — deployment or provisioning."""
    text = README.read_text().lower()
    # Compilation is verified in CI; provisioning/deployment is not.
    for phrase in ("bicep deployed", "infrastructure deployed", "provisioned in azure"):
        assert phrase not in text, f"README overclaims beyond CI: {phrase!r}"
    # The Bicep status must still be qualified as not provisioned somewhere.
    assert "not provisioned" in text


def test_docs_do_not_claim_notebooks_run_unchanged():
    """The boundary is: Silver/Gold reused; Bronze loader and validation differ."""
    docs = [
        README,
        REPO_ROOT / "docs" / "architecture" / "azure-target-architecture.md",
        REPO_ROOT / "docs" / "decisions" / "001-databricks-core-and-azure-deployment-boundary.md",
    ]
    forbidden = [
        "same databricks pipeline runs unchanged",
        "same notebooks run unchanged",
        "no notebook changes are needed",
    ]
    for doc in docs:
        text = doc.read_text().lower()
        for phrase in forbidden:
            assert phrase not in text, f"{doc.name} still claims: {phrase!r}"


def test_adr_does_not_assert_bicep_compiles():
    adr = (REPO_ROOT / "docs" / "decisions"
           / "001-databricks-core-and-azure-deployment-boundary.md").read_text().lower()
    assert "- bicep compiles" not in adr
    assert "remains unverified until the github actions build succeeds" in adr


def test_main_bicep_comment_does_not_assert_it_compiles():
    main = (REPO_ROOT / "azure" / "bicep" / "main.bicep").read_text().lower()
    assert "this template compiles" not in main


def test_docs_do_not_preemptively_claim_static_validation_is_done():
    """Static validation (incl. Bicep) is enforced in CI; docs must not state it as
    already achieved before a green CI run."""
    docs = [
        README,
        REPO_ROOT / "docs" / "architecture" / "azure-target-architecture.md",
        REPO_ROOT / "docs" / "decisions" / "001-databricks-core-and-azure-deployment-boundary.md",
    ]
    forbidden = [
        "have been statically validated",
        "configuration that is statically validated",
        "compilable bicep",
    ]
    for doc in docs:
        text = doc.read_text().lower()
        for phrase in forbidden:
            assert phrase not in text, f"{doc.name} preemptively claims: {phrase!r}"


def test_docs_do_not_claim_adf_passes_landing_path_automatically():
    """The landing path is the Databricks Job's default, not passed by ADF."""
    docs = [
        REPO_ROOT / "docs" / "architecture" / "azure-target-architecture.md",
        REPO_ROOT / "docs" / "decisions" / "001-databricks-core-and-azure-deployment-boundary.md",
    ]
    for doc in docs:
        text = doc.read_text().lower()
        assert "supplies it automatically" not in text, (
            f"{doc.name} claims ADF supplies landing_path"
        )


def test_architecture_doc_uses_current_activity_names():
    text = (REPO_ROOT / "docs" / "architecture" / "azure-target-architecture.md").read_text()
    assert "CopyEntityToLanding" not in text, "architecture doc uses a stale activity name"
    assert "CopyIncremental" in text
