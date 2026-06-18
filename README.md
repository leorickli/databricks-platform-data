# lmx-data

This is the **data layer** of **lmx**, a reference data platform on Databricks. It ingests telemetry from pluggable connectors, transforms it through a medallion architecture (land → bronze → silver → gold), and serves the curated data and connection metadata to a range of downstream consumers:

- **Frontend web app** — via the Databricks SQL API, or **Lakebase (managed Postgres) synced tables** that mirror gold/serving tables for sub-second reads with no SQL-warehouse cold start.
- **Databricks Apps** — built directly on the gold/serving layer.
- **Genie spaces** — natural-language querying over the gold tables for self-serve analytics.
- **AI/ML** — feature tables, model training, and inference on the curated silver/gold data.

The gold layer is the product: one clean, governed serving layer that any number of endpoints can attach to.

Two example connectors ship in this repo — **AMPCORE** and **Smartnode** — both built as Lakeflow Declarative Pipelines. They exist to demonstrate the architecture and the conventions; add your own connectors alongside them.

## The two repositories

**lmx** is split into two repos along one clean boundary: **[lmx-infra](https://github.com/your-account/lmx-infra) owns *where things live*; this repo (`lmx-data`) owns *what runs in them*.**

`lmx-infra` uses **Terraform** (Infrastructure as Code) to provision the foundation — AWS resources, Databricks workspaces, Unity Catalog catalogs/schemas and grants, SQL warehouses, and the Lakebase serving layer. This repo assumes those already exist and never creates them (hence the "never `CREATE SCHEMA` from this repo" rule); it deploys the connectors, transforms, and serving pipelines with **Databricks Asset Bundles (DABs)**.

Keeping them separate buys:

- **Independent lifecycles** — the foundation changes rarely and deliberately; pipelines change daily. Each moves at its own cadence.
- **Smaller blast radius** — a routine pipeline edit can never accidentally `terraform destroy` a workspace or catalog.
- **Least-privilege ownership** — the platform team holds cloud-admin and Terraform state; data engineers ship pipelines without needing either.
- **One toolchain per repo** — Terraform (plan/apply) and DABs (`bundle deploy`) stay in clean, separate CI.

## Stack

- **Databricks** — workspace, Unity Catalog, jobs, SQL warehouses
- **Databricks Asset Bundles (DABs)** — IaC for jobs, pipelines, and environments (`databricks.yml`, `resources/`)
- **Lakeflow Declarative Pipelines (LDP / SDP)** — `@dp.table` notebooks + `*.sql` gold for each connector
- **PySpark / Spark Structured Streaming** — bronze/silver transforms, Auto Loader
- **Delta Lake** with **liquid clustering (`CLUSTER BY AUTO`)** as the default partitioning strategy
- **Unity Catalog** — multi-tenant catalogs per client/env (`acme_dev/prod`, `lmx_dev/prod`)
- **GitHub Actions** — CI/CD for bundle deploys (`.github/workflows/`)

## Architecture

Medallion layers:

| Layer  | Purpose                                  | Tooling                              |
|--------|------------------------------------------|--------------------------------------|
| Land   | Raw API/file payloads in UC volumes      | Python notebooks (`*_api2land`)      |
| Bronze | Raw structured tables                    | Auto Loader / `@dp.table` (Python)   |
| Silver | Cleaned & enriched                       | PySpark / `@dp.table`                |
| Gold   | Business-ready serving layer             | **SQL only** (`*.sql` LDP)           |

Per-connector layout under `src/connectors/<name>/{batch,stream}/`. Cross-connector orchestration (pause controller, connection-status aggregation, failure alerts) lives in `src/shared/`.

## Repo layout

```
src/
  connectors/   # one folder per source system (batch/ and/or stream/)
  shared/       # b00 pause, b10/b11 connection metadata, b99 failure status
  clients/      # client-specific helpers
  lmx/          # platform-wide utilities (e.g. volume TTL cleanup)
resources/      # DAB job & pipeline definitions
databricks.yml  # DAB targets (dev/stg/prod)
```

## Common commands

```bash
# Deploy bundle
databricks bundle deploy --target lmx-dev
databricks bundle deploy --target lmx-prod

# Run a job
databricks bundle run <job_name> --target lmx-dev
```

See [CLAUDE.md](CLAUDE.md) for the full conventions (clustering, gold-SQL-only, JSON ingestion rules).
