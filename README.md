# UK Flood Monitoring – Databricks Infrastructure and Deployment

This repository provisions and deploys a real-time flood-monitoring data platform on **Databricks** using **Terraform** and **Databricks Asset Bundles**.  
It automates environment setup, governance, and data pipeline deployment for both **development** and **production**.

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/<your-org>/uk-flood-pipeline.git
cd uk-flood-pipeline

# 2. Provision infrastructure
terraform -chdir=terraform init
terraform -chdir=terraform apply -var-file=envs/dev.tfvars

# 3. Deploy Databricks bundle
databricks bundle deploy --target dev

# 4. Run job or pipeline
databricks bundle run --target dev --name uk_flood_monitoring_job
```

---

## Repository Structure

```
uk-flood-pipeline/
│
├── terraform/                  # Infrastructure-as-Code for Databricks and Azure
├── pipelines/                  # DLT pipeline definitions
├── jobs/                       # Job definitions for pipeline execution
├── notebooks/                  # DLT notebooks (Bronze → Silver → Gold)
├── .databricks/bundle/         # Environment variable overrides
├── databricks.yml              # Root Asset Bundle configuration
└── README.md                   # Documentation (this file)
```

---

## Terraform – Infrastructure Setup

Terraform provisions all Databricks and Azure components required for the platform.

### Key Files

| File                                            | Purpose                                                     |
| ----------------------------------------------- | ----------------------------------------------------------- |
| `provider.tf`                                   | Core Terraform provider configuration                       |
| `provider.azurerm.tf`                           | AzureRM provider authentication (OIDC or service principal) |
| `provider.databricks.tf`                        | Databricks provider configuration                           |
| `main.tf`                                       | Core orchestration and dependencies                         |
| `backend.tf`                                    | Remote backend configuration for state management           |
| `catalog.tf`                                    | Unity Catalog, schemas, and permissions                     |
| `cluster.tf`                                    | Cluster definitions                                         |
| `governance_groups.tf` / `governance_grants.tf` | Access control for Unity Catalog                            |
| `governance_volumes.tf`                         | UC volumes and storage links                                |
| `storage_uc.tf`                                 | External location and storage integration                   |
| `variables.tf` / `outputs.tf`                   | Input variables and outputs                                 |

### Commands

```bash
terraform -chdir=terraform init
terraform -chdir=terraform validate
terraform -chdir=terraform plan -var-file=envs/dev.tfvars
terraform -chdir=terraform apply -var-file=envs/dev.tfvars
```

### Resources Created

* Databricks workspace and linked Azure resource group
* Unity Catalog with Bronze, Silver, and Gold schemas
* External storage and UC volumes
* Databricks groups and permission grants
* Shared or single-user dev cluster

---

## Databricks Asset Bundles – Deployment

Once infrastructure is provisioned, use Asset Bundles to deploy jobs, pipelines, and notebooks to the workspace.

### Structure

```
uk-flood-pipeline/
├── databricks.yml
├── pipelines/
│   └── uk_flood_pipeline.yml
├── jobs/
│   └── uk_flood_monitoring_job.yml
└── .databricks/
    └── bundle/
        ├── dev/variable-overrides.json
        └── prod/variable-overrides.json
```

### Root Configuration (`databricks.yml`)

```yaml
bundle:
  name: uk-flood-pipeline

include:
  - pipelines/**
  - jobs/**
  - notebooks/**

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<workspace-id>.<region>.azuredatabricks.net
      root_path: /Users/<user>/flood-monitoring
    variables:
      catalog: flood_dev
      schema: bronze

  prod:
    mode: production
    workspace:
      host: https://adb-<workspace-id>.<region>.azuredatabricks.net
      root_path: /Repos/flood-monitoring
    variables:
      catalog: flood_prod
      schema: bronze
```

### Variable Overrides

`.databricks/bundle/dev/variable-overrides.json`

```json
{
  "catalog": "flood_dev",
  "schema": "bronze",
  "max_records": "100",
  "limit_per_page": "100"
}
```

### Commands

```bash
# Validate
databricks bundle validate

# Deploy to environment
databricks bundle deploy --target dev

# Run job
databricks bundle run --target dev --name uk_flood_monitoring_job
```

---

## Notebooks and Pipelines

| Notebook                 | Description                                                                                                                     |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `bronze_ingest.ipynb`    | Fetches flood-monitoring data from the UK Environment Agency API, writes to `${pipeline.catalog}.${pipeline.schema}.floods_raw` |
| `silver_transform.ipynb` | Cleans Bronze data and writes to `${pipeline.catalog}.silver.floods_clean`                                                      |
| `gold_aggregates.ipynb`  | Aggregates latest alerts and writes to `${pipeline.catalog}.gold.latest_alert`                                                  |

`pipelines/uk_flood_pipeline.yml` defines the Delta Live Tables pipeline referencing these notebooks.

---

## Development Workflow

1. **Provision infrastructure** with Terraform.
2. **Develop locally** using Databricks Connect.
3. **Deploy updates** using Asset Bundles.
4. **Automate CI/CD** via GitHub Actions (plan, apply, deploy).

---

## Unity Catalog and Governance

All tables are created under Unity Catalog with environment separation:

* `flood_dev` and `flood_prod` catalogs.
* Bronze, Silver, and Gold schemas.
* Permissions managed through Terraform governance modules.

---

## Next Steps

* Add data quality expectations to Delta Live Tables.
* Integrate system tables for observability.
* Extend CI/CD with automated tests and rollback.
* Add ODIS Logger for telemetry and event tracing.

---

### Author

Sheldon Lee Kemper


