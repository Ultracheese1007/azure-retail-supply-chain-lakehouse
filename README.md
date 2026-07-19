# Azure Retail & Supply Chain Lakehouse

An end-to-end data engineering project for a multi-warehouse e-commerce retailer:
a governed lakehouse that manages customer history, produces trustworthy sales
analytics, and tracks inventory movement across warehouses.

The data-transformation and orchestration core is built on **Databricks,
PySpark, Delta Lake, Unity Catalog and Databricks Workflows**, and has been
implemented and **executed** end to end (on Databricks, with managed storage).
It is fronted by an **Azure Data Factory** metadata-driven ingestion layer
landing into **ADLS Gen2**, with infrastructure defined in **Bicep**. The Azure
Databricks / ADF / ADLS target is **defined and statically checked, not
provisioned**: Bicep compilation is configured in CI and must pass before the
Azure assets are considered statically validated.

## Business problem

A multi-warehouse retailer holds operational data across several systems: a
customer master that changes over time, a product catalog, warehouse locations,
order headers and line items, and a continuous stream of inventory movements.

Reporting directly against these systems is slow, couples analytics to
transactional load, and gives no reliable view of history — for example, what a
customer's attributes were at the time an order was placed. This project builds a
lakehouse that preserves customer history for point-in-time analysis, produces
consistent sales facts, tracks inventory movement per warehouse, and makes
data-quality expectations explicit and enforced.

## Architecture

```
Azure SQL (operational source)
        |
        v
Azure Data Factory  — metadata-driven full & watermark-incremental ingestion
        |
        v
ADLS Gen2 landing zone  (Parquet, partitioned by entity and load date)
        |
        v
Azure Databricks — Bronze -> Silver -> Gold -> validation  (PySpark, Delta, Unity Catalog)
        |
        v
Gold star schema — Type 2 customer dimension, point-in-time sales fact, inventory fact
```

The demo and Azure targets use separate Bronze loaders and validation tasks; the
Silver and Gold transformation core is reused unchanged. The demo path reads
generated CSVs from a managed Volume; the Azure path reads ADF-produced Parquet
from an ADLS Gen2 landing zone.

## Engineering capabilities

- **Metadata-driven ingestion** — one parameterized pipeline serves all six
  entities from a control table, rather than a pipeline per table.
- **Watermark incremental loading** — incremental entities copy only rows past
  the last successful watermark; the watermark advances only after a successful
  copy, so a failure reprocesses rather than skips.
- **Slowly Changing Dimension Type 2** — customer history via a two-stage
  expire-and-insert with **null-safe change detection**, so a `NULL -> value`
  change is not missed.
- **Point-in-time dimensional join** — the sales fact joins each order to the
  customer version in effect *at order time*, using a half-open validity window,
  preventing the fan-out that a naive key join would cause.
- **Quarantine and deduplication** — invalid rows are routed to a quarantine
  schema with a reason; duplicates collapse to one Silver row.
- **Executable validation gate** — a validation task runs a suite of checks and
  fails the run on any violation, rather than logging a warning.
- **Idempotent reruns** — re-running the pipeline produces no extra customer
  versions and no duplicated facts.
- **Failure propagation** — the multi-task Workflow blocks dependents of a failed
  task and ends in the validation gate.

## Technology stack

Python · SQL · PySpark · Spark SQL · Delta Lake · Unity Catalog · Databricks
Workflows · Azure Data Factory · ADLS Gen2 · Bicep · GitHub Actions

## Implementation status

| Component | Status |
|---|---|
| Databricks transformation core (Bronze→Gold) | Implemented and executed |
| SCD Type 2 and point-in-time modelling | Implemented and validated |
| Databricks multi-task Workflow | Implemented and executed |
| Data-quality validation gate | Implemented and validated |
| ADF metadata-driven ingestion assets | Defined; static checks configured in CI |
| ADLS Gen2 landing configuration | Defined |
| Azure infrastructure (ADF, ADLS Gen2, Databricks) | Defined in Bicep; not provisioned |

The Databricks core was executed against generated data, with evidence under
[`docs/evidence/milestone-3`](docs/evidence/milestone-3) and
[`docs/evidence/milestone-4`](docs/evidence/milestone-4). The Azure ingestion and
infrastructure layer is deployment-ready configuration. Its checks — JSON
validity, ADF cross-references, secret scanning, and **Bicep compilation** — are
configured in CI (`.github/workflows/ci.yml`) and must pass before the Azure
assets are considered statically validated. It has **not** been provisioned, no
ADF pipeline has run against a live source, and the core was **not** executed on
a provisioned Azure Databricks workspace.

## Repository layout

```
databricks/          Bronze / Silver / Gold / quality notebooks and the Workflow
src/retail_lakehouse Deterministic source generator + shared transformation logic
tests/unit           Spark-free unit tests (transformation logic + Azure assets)
azure/adf            Data Factory linked services, datasets and pipelines
azure/config         Ingestion metadata for the six entities
azure/sql            Idempotent ingestion control table + watermark procedure
azure/bicep          ADLS Gen2, Data Factory and Databricks infrastructure
docs/architecture    Azure target architecture
docs/decisions       Architecture decision records
docs/azure-deployment-guide.md
docs/evidence        Databricks execution evidence
```

## Running the Databricks core

The pipeline runs on Databricks with managed storage. In order:

1. `databricks/setup/01_create_lakehouse` — catalog, schemas, landing volume
2. `databricks/bronze/01_generate_demo_batches` — write the two source batches
3. `databricks/bronze/02_load_bronze` — load Bronze with audit columns
4. `databricks/silver/01`–`06` — clean, quarantine, and build SCD2 history
5. `databricks/gold/01`–`03` — dimensions and facts
6. `databricks/quality/01_validate_pipeline` — the validation gate

Or import `databricks/workflows/lakehouse_pipeline.json` as a multi-task Job and
run it end to end. Deploying the Azure layer is described in
[`docs/azure-deployment-guide.md`](docs/azure-deployment-guide.md).

## Local checks

```bash
python -m pip install -e ".[dev]"
python -m ruff check .
PYTHONPATH=src python -m pytest -q
```

## Data entities

`customers`, `products`, `warehouses`, `orders`, `order_items`,
`inventory_events`. Load types per entity, and the reasoning behind each, are in
[`docs/architecture/azure-target-architecture.md`](docs/architecture/azure-target-architecture.md).

## Known limitations

- The source data is generated and portfolio-scale, not a production workload.
- Gold tables are rebuilt deterministically from Silver rather than incrementally
  loaded.
- The Azure infrastructure and ADF ingestion are defined, with static
  validation configured in CI, but **not provisioned**; no live cloud run has
  occurred.
- No production SLA, throughput, or cost figures are claimed or measured.
- The Azure landing design currently treats one calendar date as one logical
  ingestion batch. A production implementation with multiple runs per day should
  add a run ID or ingestion-timestamp partition to preserve intra-day SCD changes.
