# ADR 001: Databricks core executed; Azure represented as deployment assets

## Status

Accepted.

## Context

The project needs to demonstrate an Azure-oriented retail data platform, but no
Azure subscription is available to provision and run cloud infrastructure. There
is a temptation to either (a) describe the whole thing as if it were deployed, or
(b) not build the Azure layer at all. Both are poor: the first is dishonest, the
second leaves the project looking like a Databricks-only exercise.

## Decision

Split the project along a clear boundary:

- The **Databricks transformation and orchestration core** is implemented and
  **executed** against generated data, with evidence captured under
  `docs/evidence/`.
- The **Azure ingestion and infrastructure layer** (ADF, ADLS Gen2, Bicep) is
  implemented as **deployment-oriented configuration with static validation
  enforced in CI** — valid JSON, resolvable cross-references, Bicep compilation,
  and enforced absence of secrets — but is **not provisioned**.

The two are decoupled by a single seam: the Databricks `landing_path` parameter.
Locally it points at a managed Unity Catalog Volume; the Azure orchestration
pipeline uses a separate Bronze loader that reads ADF-produced Parquet from an
`abfss://` ADLS Gen2 path, and a generic validation task. The Silver and Gold
transformation core is reused unchanged between the two targets.

## What has and has not been validated

Validated by execution (Databricks):

- Bronze→Silver→Gold transformation, SCD Type 2, point-in-time joins;
- the multi-task Workflow, including retries and upstream-failure propagation;
- the executable validation gate and idempotent reruns.

Validated statically only (Azure):

- ADF JSON parses and its linked-service, dataset and pipeline references
  resolve;
- ingestion metadata matches the six real entities and uses only real watermark
  columns;
- Bicep compilation is configured in CI and remains unverified until the GitHub Actions build succeeds;
- no secrets or environment-specific identifiers are present.

Not validated at all:

- any live ADF run, ADLS ingestion, or Azure deployment.

## How a future Azure deployment would replace the Volume

1. Deploy the Bicep infrastructure.
2. Grant the ADF and Databricks managed identities access to Azure SQL and ADLS.
3. Create the control table and publish the ADF assets.
4. Point the Databricks job's `landing_path` at the ADLS landing zone by running
   configured as the Azure Databricks Job's `landing_path` default during
   deployment (ADF triggers the job by ID and passes no per-run parameters).

The managed Volume path is thus a development stand-in for the ADLS Gen2 landing
zone, swapped by parameter rather than by rewrite.

## Why this distinction matters

An interviewer can trust every claim in this repository because each one is
labelled by how it was verified. Presenting static configuration as a live
deployment would be easy to disprove and would undermine the credible parts. The
boundary is stated explicitly in the README status table, the architecture
document and the deployment guide.

## Consequences

- The README must keep the status table accurate as components change.
- Any future real deployment moves rows from "statically validated" to
  "executed" and should add its own evidence, mirroring `docs/evidence/`.
