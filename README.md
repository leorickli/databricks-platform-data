# lmx-data

**lmx** is a reference data platform on Databricks. It ingests telemetry from pluggable connectors, transforms it through a medallion architecture (land → bronze → silver → gold), and exposes data + connection metadata to a frontend web app via the Databricks SQL API.

Two example connectors ship in this repo — **AMPCORE** and **Smartnode** — both built as Lakeflow Declarative Pipelines. They exist to demonstrate the architecture and the conventions; add your own connectors alongside them.

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
| Gold   | Business-ready, frontend-facing          | **SQL only** (`*.sql` LDP)           |

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
