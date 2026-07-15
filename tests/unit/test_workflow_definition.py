"""Unit tests for the Databricks workflow definition.

A job definition is code: it can reference a task that does not exist, point at
a notebook that was renamed, or carry someone's cluster id into a repository.
None of that shows up until a run fails, so it is checked here instead.

These tests read the JSON directly and need neither Spark nor a workspace.
"""
from __future__ import annotations

import json
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).parents[2]
WORKFLOW_PATH = REPO_ROOT / "databricks" / "workflows" / "lakehouse_pipeline.json"


@pytest.fixture(scope="module")
def workflow() -> dict:
    return json.loads(WORKFLOW_PATH.read_text())


@pytest.fixture(scope="module")
def tasks(workflow) -> dict[str, dict]:
    return {t["task_key"]: t for t in workflow["tasks"]}


def test_workflow_definition_is_valid_json(workflow):
    assert workflow["name"]
    assert workflow["tasks"]


def test_task_keys_are_unique(workflow):
    keys = [t["task_key"] for t in workflow["tasks"]]
    assert len(keys) == len(set(keys))


def test_dependencies_reference_existing_tasks(tasks):
    for key, task in tasks.items():
        for dep in task.get("depends_on", []):
            assert dep["task_key"] in tasks, f"{key} depends on unknown task {dep['task_key']}"


def test_dag_is_acyclic(tasks):
    """A cycle would make the job undeployable; catch it here rather than in the UI."""
    visiting: set[str] = set()
    done: set[str] = set()

    def visit(key: str, path: list[str]) -> None:
        if key in done:
            return
        assert key not in visiting, f"cycle detected: {' -> '.join(path + [key])}"
        visiting.add(key)
        for dep in tasks[key].get("depends_on", []):
            visit(dep["task_key"], path + [key])
        visiting.discard(key)
        done.add(key)

    for key in tasks:
        visit(key, [])


def test_every_notebook_path_exists_in_the_repo(tasks):
    """The job references notebooks by repo-relative path; they must actually be there."""
    for key, task in tasks.items():
        notebook = task["notebook_task"]["notebook_path"]
        path = REPO_ROOT / f"{notebook}.py"
        assert path.exists(), f"task {key} points at missing notebook {notebook}"


def test_notebooks_are_sourced_from_git(tasks):
    """GIT source keeps workspace-specific paths out of the definition entirely."""
    for key, task in tasks.items():
        assert task["notebook_task"].get("source") == "GIT", f"{key} is not sourced from Git"


def test_no_hardcoded_compute_or_personal_environment():
    """Free Edition is serverless-only, and other people's clusters/emails are not ours."""
    raw = WORKFLOW_PATH.read_text()
    forbidden_tokens = (
        "existing_cluster_id",
        "job_clusters",
        "@gmail",
        "@hotmail",
        "/Workspace/Users",
    )
    for forbidden in forbidden_tokens:
        assert forbidden not in raw, f"workflow contains {forbidden!r}"


def test_pipeline_starts_from_generation_and_setup(tasks):
    roots = {k for k, t in tasks.items() if not t.get("depends_on")}
    assert roots == {"generate_source_data", "create_lakehouse"}


def test_validation_is_the_final_gate(tasks):
    """Nothing may run after validation, or the gate would not gate anything."""
    downstream = {
        dep["task_key"] for t in tasks.values() for dep in t.get("depends_on", [])
    }
    assert "validate" not in downstream
    assert tasks["validate"]["depends_on"]


def test_gold_waits_for_every_silver_input(tasks):
    """Gold must not build from half-written Silver tables."""
    dim_deps = {d["task_key"] for d in tasks["gold_dimensions"]["depends_on"]}
    assert {"clean_products", "clean_warehouses", "customer_scd2"} <= dim_deps

    sales_deps = {d["task_key"] for d in tasks["gold_sales_fact"]["depends_on"]}
    assert {"gold_dimensions", "clean_orders", "clean_order_items"} <= sales_deps

    inventory_deps = {d["task_key"] for d in tasks["gold_inventory_fact"]["depends_on"]}
    assert {"gold_dimensions", "clean_inventory_events"} <= inventory_deps


def test_validation_does_not_retry(tasks):
    """Retries are for transient infrastructure faults; a failed check is deterministic."""
    assert tasks["validate"].get("max_retries", 0) == 0


def test_other_tasks_retry_transient_failures(tasks):
    for key, task in tasks.items():
        if key == "validate":
            continue
        assert task.get("max_retries", 0) >= 1, f"{key} has no retries configured"


def test_concurrent_runs_are_disabled(workflow):
    """The pipeline overwrites tables; two runs at once would race each other."""
    assert workflow["max_concurrent_runs"] == 1


def test_parameters_are_declared_and_passed_through(workflow, tasks):
    declared = {p["name"] for p in workflow["parameters"]}
    assert declared == {"catalog", "landing_path"}

    for key, task in tasks.items():
        for value in task["notebook_task"].get("base_parameters", {}).values():
            if value.startswith("{{job.parameters."):
                name = value.removeprefix("{{job.parameters.").removesuffix("}}")
                assert name in declared, f"{key} references undeclared parameter {name}"


def test_every_task_that_touches_the_catalog_receives_it(tasks):
    """Only the generator writes files without needing a catalog."""
    for key, task in tasks.items():
        if key == "generate_source_data":
            continue
        params = task["notebook_task"].get("base_parameters", {})
        assert "catalog" in params, f"{key} does not receive the catalog parameter"
