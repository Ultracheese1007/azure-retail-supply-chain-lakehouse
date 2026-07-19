"""Static validation of the Azure integration assets.

None of this is deployed, so nothing here talks to Azure. The tests check the
things that would otherwise only surface at deploy time: malformed JSON, ADF
references pointing at assets that do not exist, drift between the ingestion
metadata and the six business entities the pipeline actually processes, and any
secret or environment-specific value accidentally committed.

The generator's own column list is the source of truth for which watermark
columns are legitimate, so the metadata cannot reference a column that the data
does not have.
"""
from __future__ import annotations

import json
import pathlib
import re

import pytest

from retail_lakehouse import generate_source_data as gen

REPO_ROOT = pathlib.Path(__file__).parents[2]
AZURE = REPO_ROOT / "azure"
ADF = AZURE / "adf"

ENTITIES = ["customers", "products", "warehouses", "orders", "order_items", "inventory_events"]


def _load(path: pathlib.Path) -> dict:
    return json.loads(path.read_text())


def _all_json_files() -> list[pathlib.Path]:
    return sorted(AZURE.rglob("*.json"))


# ---------------------------------------------------------------------------
# JSON validity and presence
# ---------------------------------------------------------------------------

def test_azure_directory_exists():
    assert AZURE.is_dir()


@pytest.mark.parametrize("path", _all_json_files(), ids=lambda p: p.name)
def test_every_azure_json_parses(path):
    _load(path)


def test_required_adf_assets_exist():
    required = [
        "linkedService/ls_azure_sql.json",
        "linkedService/ls_adls_gen2.json",
        "linkedService/ls_azure_databricks.json",
        "dataset/ds_azure_sql_table.json",
        "dataset/ds_adls_landing.json",
        "pipeline/pl_metadata_ingestion.json",
        "pipeline/pl_orchestrate_lakehouse.json",
    ]
    for rel in required:
        assert (ADF / rel).is_file(), f"missing ADF asset: {rel}"


# ---------------------------------------------------------------------------
# Cross-file references resolve
# ---------------------------------------------------------------------------

def _reference_names(node, kind: str) -> set[str]:
    """Collect every referenceName under objects whose type is <kind>Reference."""
    found: set[str] = set()
    if isinstance(node, dict):
        if node.get("type") == f"{kind}Reference" and "referenceName" in node:
            found.add(node["referenceName"])
        for value in node.values():
            found |= _reference_names(value, kind)
    elif isinstance(node, list):
        for item in node:
            found |= _reference_names(item, kind)
    return found


def _asset_names(subdir: str) -> set[str]:
    return {_load(p)["name"] for p in (ADF / subdir).glob("*.json")}


def test_linked_service_references_resolve():
    available = _asset_names("linkedService")
    for path in ADF.rglob("*.json"):
        referenced = _reference_names(_load(path), "LinkedService")
        missing = referenced - available
        assert not missing, f"{path.name} references missing linked services: {missing}"


def test_dataset_references_resolve():
    available = _asset_names("dataset")
    for path in ADF.rglob("*.json"):
        referenced = _reference_names(_load(path), "Dataset")
        missing = referenced - available
        assert not missing, f"{path.name} references missing datasets: {missing}"


def test_pipeline_references_resolve():
    available = _asset_names("pipeline")
    for path in ADF.rglob("*.json"):
        referenced = _reference_names(_load(path), "Pipeline")
        missing = referenced - available
        assert not missing, f"{path.name} references missing pipelines: {missing}"


# ---------------------------------------------------------------------------
# Ingestion metadata is consistent with the real entities and schema
# ---------------------------------------------------------------------------

def test_metadata_covers_exactly_the_six_entities():
    config = _load(AZURE / "config" / "ingestion_entities.json")
    names = [e["entity_name"] for e in config["entities"]]
    assert sorted(names) == sorted(ENTITIES)
    assert len(names) == len(set(names)), "duplicate entity in metadata"


def test_incremental_entities_declare_a_real_watermark_column():
    config = _load(AZURE / "config" / "ingestion_entities.json")
    for entity in config["entities"]:
        name = entity["entity_name"]
        if entity["load_type"] == "incremental":
            column = entity["watermark_column"]
            assert column, f"{name} is incremental but has no watermark_column"
            assert column in gen.COLUMNS[name], (
                f"{name} watermark_column {column!r} is not a real column of the source"
            )
        else:
            assert entity["watermark_column"] is None, (
                f"{name} is a full load but declares a watermark_column"
            )


def test_full_load_entities_have_no_time_column_to_use():
    """A full load is only justified when the source truly lacks a usable watermark."""
    entities = _load(AZURE / "config" / "ingestion_entities.json")["entities"]
    config = {e["entity_name"]: e for e in entities}
    time_like = re.compile(r"(_at|_timestamp)$")
    for name, entity in config.items():
        if entity["load_type"] == "full":
            time_columns = [c for c in gen.COLUMNS[name] if time_like.search(c)]
            assert not time_columns, (
                f"{name} is a full load but has time column(s) {time_columns} usable as a watermark"
            )


def test_sql_control_seed_matches_the_config_entities():
    sql = (AZURE / "sql" / "001_create_ingestion_control.sql").read_text()
    for name in ENTITIES:
        assert f"'{name}'" in sql, f"control table seed is missing entity {name}"


# ---------------------------------------------------------------------------
# No secrets, no environment-specific values, no leftover foreign entities
# ---------------------------------------------------------------------------

def _all_azure_text() -> str:
    parts = []
    for path in AZURE.rglob("*"):
        if path.is_file() and path.suffix in {".json", ".bicep", ".bicepparam", ".sql"}:
            parts.append(path.read_text())
    return "\n".join(parts)


def test_no_encrypted_credentials():
    assert "encryptedCredential" not in _all_azure_text()


def test_no_secrets_or_personal_identifiers():
    text = _all_azure_text()
    forbidden = [
        "dapi",                       # Databricks PAT prefix
        "AccountKey=",
        "password=",
        "pwd=",
        "@gmail",
        "@hotmail",
        "/Workspace/Users/",
        "existing_cluster_id",
    ]
    for token in forbidden:
        assert token.lower() not in text.lower(), f"forbidden value present: {token!r}"


def test_no_hardcoded_subscription_or_tenant_guid():
    """Role-definition GUIDs are allowed; subscription/tenant GUIDs are not."""
    text = _all_azure_text()
    for keyword in ("subscriptionId", "tenantId", "subscription-id", "tenant-id"):
        assert keyword not in text, f"{keyword} should not be hard-coded"


def test_no_hardcoded_storage_account_url():
    """Storage hosts must be built from parameters, never a literal account name."""
    text = _all_azure_text()
    literal_hosts = re.findall(r"https://[a-z0-9]+\.dfs\.core\.windows\.net", text)
    assert literal_hosts == [], f"hard-coded storage host(s): {literal_hosts}"


def _azure_text_without_prose() -> str:
    """Concatenate Azure asset text with human-prose fields removed, so tests match
    actual SQL / references / paths rather than words in descriptions."""
    prose_keys = {"description", "annotations"}
    chunks = []

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k in prose_keys:
                    continue
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, str):
            chunks.append(node)

    for path in AZURE.rglob("*.json"):
        walk(_load(path))
    for path in AZURE.rglob("*"):
        if path.suffix in {".bicep", ".bicepparam", ".sql"}:
            chunks.append(path.read_text())
    return "\n".join(chunks)


def test_no_superstore_entity_names_remain():
    text = _azure_text_without_prose().lower()
    for foreign in ("branches", "categories", "order_details", "superstore"):
        assert not re.search(rf"\\b{foreign}\\b", text), f"foreign entity name present: {foreign}"


def test_no_hardcoded_order_date_range():
    """The reference project filtered orders by a fixed date window; ours must not."""
    text = _all_azure_text()
    assert "2021-01-01" not in text
    assert "BETWEEN" not in text.upper() or "2021" not in text


# ---------------------------------------------------------------------------
# Watermark discipline and orchestration ordering
# ---------------------------------------------------------------------------

def _incremental_branch_activities():
    pipeline = _load(ADF / "pipeline" / "pl_metadata_ingestion.json")
    foreach = next(a for a in pipeline["properties"]["activities"] if a["type"] == "ForEach")
    branch = next(a for a in foreach["typeProperties"]["activities"] if a["type"] == "IfCondition")
    true_acts = {a["name"]: a for a in branch["typeProperties"]["ifTrueActivities"]}
    false_acts = {a["name"]: a for a in branch["typeProperties"]["ifFalseActivities"]}
    return branch, true_acts, false_acts


def test_load_type_is_branched_with_ifcondition():
    branch, true_acts, false_acts = _incremental_branch_activities()
    expr = branch["typeProperties"]["expression"]["value"]
    assert expr == "@equals(item().load_type, 'incremental')"
    # incremental branch has watermark + incremental copy; full branch has only a full copy
    assert "GetHighWatermark" in true_acts
    assert "CopyIncremental" in true_acts
    assert "CopyFull" in false_acts
    assert "GetHighWatermark" not in false_acts


def _sql_and_procs(activities: dict) -> str:
    """Collect only executable SQL text and stored-procedure names from activities."""
    parts = []

    def walk(node):
        if isinstance(node, dict):
            if "sqlReaderQuery" in node:
                q = node["sqlReaderQuery"]
                parts.append(q["value"] if isinstance(q, dict) else q)
            if "storedProcedureName" in node:
                parts.append(node["storedProcedureName"])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(activities)
    return "\n".join(parts)


def test_full_load_branch_does_not_compute_max_or_update_watermark():
    _, _, false_acts = _incremental_branch_activities()
    executable = _sql_and_procs(false_acts)
    assert "MAX(" not in executable, "full-load branch must not compute MAX()"
    assert "update_watermark" not in executable, "full-load branch must not advance the watermark"


def test_watermark_is_only_advanced_after_a_successful_copy():
    _, true_acts, _ = _incremental_branch_activities()
    advance = true_acts["AdvanceWatermarkIfPresent"]
    depends = {d["activity"]: d["dependencyConditions"] for d in advance["dependsOn"]}
    assert depends.get("CopyIncremental") == ["Succeeded"], (
        "watermark must advance only after the incremental copy succeeds"
    )


def test_watermark_advance_is_guarded_against_null_high_watermark():
    _, true_acts, _ = _incremental_branch_activities()
    advance = true_acts["AdvanceWatermarkIfPresent"]
    assert advance["type"] == "IfCondition"
    expr = advance["typeProperties"]["expression"]["value"]
    assert "null" in expr, "a null high watermark must not overwrite the stored watermark"


def test_orchestration_runs_databricks_only_after_ingestion_succeeds():
    pipeline = _load(ADF / "pipeline" / "pl_orchestrate_lakehouse.json")
    activities = {a["name"]: a for a in pipeline["properties"]["activities"]}
    databricks = activities["RunDatabricksLakehouseJob"]
    depends = {d["activity"]: d["dependencyConditions"] for d in databricks["dependsOn"]}
    assert depends.get("RunMetadataIngestion") == ["Succeeded"]


def test_databricks_activity_does_not_pass_job_parameters():
    """Passing jobParameters would require a Self-hosted Integration Runtime, which
    this portfolio deliberately avoids; the landing path is a job-level default."""
    pipeline = _load(ADF / "pipeline" / "pl_orchestrate_lakehouse.json")
    databricks = next(
        a for a in pipeline["properties"]["activities"] if a["type"] == "DatabricksJob"
    )
    assert "jobParameters" not in databricks["typeProperties"]


def test_no_schedule_trigger_is_committed():
    """The trigger is configured post-deployment; none is committed."""
    assert not (ADF / "trigger").exists()


def test_no_asset_uses_pipeline_parameters_in_trigger_scope():
    """@pipeline().parameters.* is invalid in trigger scope; ensure no trigger file
    reintroduces it."""
    trigger_dir = ADF / "trigger"
    if not trigger_dir.exists():
        return
    for path in trigger_dir.glob("*.json"):
        assert "@pipeline().parameters" not in path.read_text(), (
            f"{path.name} uses @pipeline() in trigger scope"
        )


# ---------------------------------------------------------------------------
# Bicep entry point and modules exist
# ---------------------------------------------------------------------------

def test_bicep_entrypoint_and_modules_exist():
    bicep = AZURE / "bicep"
    assert (bicep / "main.bicep").is_file()
    for module in ("storage.bicep", "data_factory.bicep", "databricks.bicep"):
        assert (bicep / "modules" / module).is_file(), f"missing bicep module {module}"
    assert (bicep / "parameters" / "dev.bicepparam").is_file()


def test_main_bicep_references_only_existing_modules():
    main = (AZURE / "bicep" / "main.bicep").read_text()
    for module_ref in re.findall(r"modules/([a-z_]+\.bicep)", main):
        assert (AZURE / "bicep" / "modules" / module_ref).is_file(), (
            f"main.bicep references missing module {module_ref}"
        )


# ---------------------------------------------------------------------------
# loadDate must be evaluated at runtime, never in a parameter default
# ---------------------------------------------------------------------------

def test_no_pipeline_parameter_default_uses_dynamic_expressions():
    """ADF does not evaluate expressions in parameter defaultValue; a dynamic
    default would land in a literal folder like load_date=@formatDateTime(...)."""
    for path in (ADF / "pipeline").glob("*.json"):
        params = _load(path)["properties"].get("parameters", {})
        for name, spec in params.items():
            default = str(spec.get("defaultValue", ""))
            assert "utcNow" not in default, f"{path.name}.{name} default uses utcNow()"
            assert "formatDateTime" not in default, (
                f"{path.name}.{name} default uses formatDateTime()"
            )


def test_orchestration_computes_load_date_at_runtime():
    pipeline = _load(ADF / "pipeline" / "pl_orchestrate_lakehouse.json")
    execute = next(
        a for a in pipeline["properties"]["activities"] if a["type"] == "ExecutePipeline"
    )
    load_date = execute["typeProperties"]["parameters"]["loadDate"]
    assert "utcNow" in load_date and "formatDateTime" in load_date, (
        "the runtime load date must be computed in the activity expression"
    )
    assert "empty(pipeline().parameters.loadDate)" in load_date, (
        "an explicitly supplied loadDate (backfill) must be respected"
    )
