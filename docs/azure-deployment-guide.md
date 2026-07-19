# Azure deployment guide

This guide describes how the Azure assets in `azure/` would be deployed. The
commands are provided as templates. **They have not been executed** — no Azure
subscription is attached to this project, and no resources have been provisioned.

## Prerequisites

- Azure CLI (`az`) with the Bicep CLI (`az bicep`)
- An Azure subscription and a resource group
- Permission to create storage accounts, a Data Factory, an Azure Databricks
  workspace, and role assignments in the target resource group
- A target Unity Catalog metastore that provides default managed storage — or,
  failing that, create the target catalog with an explicit managed location
  before running the job, so `CREATE CATALOG` and the managed volume succeed

## 1. Compile the infrastructure

Bicep can be validated without an Azure login:

```bash
az bicep build --file azure/bicep/main.bicep
```

This is the command that CI runs. It confirms the templates are syntactically
valid and internally consistent.

## 2. Deploy the infrastructure (example)

```bash
az group create --name rg-retaillh-dev --location westeurope

az deployment group create \
  --resource-group rg-retaillh-dev \
  --template-file azure/bicep/main.bicep \
  --parameters azure/bicep/parameters/dev.bicepparam
```

The deployment outputs the storage account name, ADLS DFS endpoint, Data Factory
name and principal ID, and the Databricks workspace URL and ID. These feed the
ADF and orchestration parameters below.

## 3. Grant data-plane permissions

The Bicep assigns, via role assignments in the template:

- **Storage Blob Data Contributor** on the storage account to both the Data
  Factory managed identity and the Databricks Access Connector;
- **Contributor** on the Databricks workspace to the Data Factory managed
  identity, so ADF can invoke the Databricks Job.

The following steps are **not** expressible in Bicep and are manual
post-deployment actions:

- **Azure SQL:** create a contained database user for the Data Factory managed
  identity and grant it read on the source schema, for example:

  ```sql
  CREATE USER [retaillh-dev-adf] FROM EXTERNAL PROVIDER;
  ALTER ROLE db_datareader ADD MEMBER [retaillh-dev-adf];
  GRANT EXECUTE ON SCHEMA::control TO [retaillh-dev-adf];
  ```

- **Databricks Job permission:** in the workspace, grant the Data Factory
  managed identity **CAN MANAGE RUN** on the Databricks Job it triggers.
  Workspace job ACLs are not part of the Azure resource model, so this cannot be
  set in Bicep.

- **Databricks Unity Catalog storage access:** register the Access Connector as
  a **storage credential**, then create an **external location** over the
  landing container (`abfss://landing@<storage-account>.dfs.core.windows.net/`).
  This is what lets the Databricks Job read the ADLS landing zone.

## 4. Create the ingestion control table

Run the control script against the Azure SQL database:

```bash
sqlcmd -S <server>.database.windows.net -d <database> \
  -G -i azure/sql/001_create_ingestion_control.sql
```

The script is idempotent — it creates `control.ingestion_metadata`, seeds the
six entities via MERGE, and creates the `control.update_watermark` procedure.

## 5. Publish the ADF assets

Import the JSON under `azure/adf/` into the Data Factory (via the ADF UI's
import, or the ARM/Git integration). Supply these as parameters — none are
committed:

- `sqlServerName`, `sqlDatabaseName`
- `storageAccountName`, `containerName`
- `databricksWorkspaceUrl`, `databricksWorkspaceResourceId`
- `databricksJobId`

## Azure Databricks serverless prerequisites

The Azure-target Job (`azure_lakehouse_pipeline.json`) declares no cluster, so it
runs on serverless compute. For that to work the workspace must:

- have **Unity Catalog enabled** (the pipeline uses a catalog and managed volume);
- be in an **Azure region that supports serverless workflows**.

If the target region does not support serverless, add a `job_clusters` block to
the Job and reference it from each task.

## 6. Create the Azure Databricks Job and set its ID

Create the multi-task Job from
`databricks/workflows/azure_lakehouse_pipeline.json` — the Azure-target workflow,
which loads Bronze from ADLS (`03_load_bronze_from_adls`) and validates
generically (`02_validate_generic`), rather than generating demo data.

When creating the job, set its **`landing_path` job parameter default** to the
ADLS path, e.g. `abfss://landing@<storage-account>.dfs.core.windows.net/retail`.
Because the landing path is a job-level default, the ADF `DatabricksJob` activity
triggers the job **by ID with no per-run `jobParameters`** — which avoids
requiring a Self-hosted Integration Runtime. Pass the job's numeric ID to the
orchestration pipeline as `databricksJobId`.

## 7. Run the orchestration

Trigger `pl_orchestrate_lakehouse`. It:

1. runs metadata ingestion into ADLS;
2. runs the pre-configured Databricks Job (by ID) only if ingestion succeeded.

A schedule is intentionally not included in the repository. Configure a schedule
trigger and its environment-specific pipeline parameter values (server names,
storage account, workspace URL, job ID) in the Data Factory after deployment.

## Expected failure and recovery behaviour

- If a source copy fails, that entity's watermark is not advanced; the next run
  reprocesses the same range. No data is skipped.
- If ingestion fails overall, the Databricks job does not start.
- If a Databricks task fails, its dependents do not run, and the validation gate
  fails the run rather than publishing partial results.

## What this guide does not claim

- No `az deployment` has been run.
- No ADF pipeline has executed against a live source.
- No ADLS ingestion has occurred.
- The Databricks core, by contrast, has been executed — see
  `docs/evidence/milestone-3` and `docs/evidence/milestone-4`.
