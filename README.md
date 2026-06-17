# dataplatformx-data

Databricks data platform for the **Data Platform Exchange (DPX)**. Ingests energy telemetry from a growing roster of connectors (Wattflow, Ecosphere, Voltcore, Sunpeak, Solarflow, TRACKSYS, AMPCORE, Smartnode, and many more), transforms it through a medallion architecture (land → bronze → silver → gold), and exposes data + connection metadata to a frontend web app via the Databricks SQL API.

## Stack

- **Databricks** — workspace, Unity Catalog, jobs, SQL warehouses
- **Databricks Asset Bundles (DABs)** — IaC for jobs, pipelines, and environments (`databricks.yml`, `resources/`)
- **Lakeflow Declarative Pipelines (LDP / SDP)** — `@dp.table` notebooks for newer connectors (e.g. AMPCORE)
- **dbt** — gold-layer models for legacy connectors (`dbt/`)
- **PySpark / Spark Structured Streaming** — bronze/silver transforms, Auto Loader, Kafka & Kinesis sources
- **Delta Lake** with **liquid clustering (`CLUSTER BY AUTO`)** as the default partitioning strategy
- **Unity Catalog** — multi-tenant catalogs per client/env (`acme_dev/prod`, `globex_dev/prod`, `dpx_dev/prod`)
- **GitHub Actions** — CI/CD for bundle deploys (`.github/workflows/`)

## Architecture

Medallion layers:

| Layer  | Purpose                                  | Tooling                              |
|--------|------------------------------------------|--------------------------------------|
| Land   | Raw API/file payloads in UC volumes      | Python notebooks (`*_api2land`)      |
| Bronze | Raw structured tables                    | Auto Loader / `@dp.table` (Python)   |
| Silver | Cleaned & enriched                       | PySpark / `@dp.table`                |
| Gold   | Business-ready, frontend-facing          | **SQL only** (dbt or `*.sql` LDP)    |

Per-connector layout under `src/connectors/<name>/{batch,stream}/`. Cross-connector orchestration (pause controller, connection-status aggregation, failure alerts) lives in `src/shared/`.

## Repo layout

```
src/
  connectors/   # one folder per source system (batch/ and/or stream/)
  shared/       # b00 pause, b10/b11 connection metadata, b99 failure status
  clients/      # client-specific helpers (ACME, GLOBEX)
  dpx/          # DPX-wide utilities
dbt/            # gold models for legacy connectors
resources/      # DAB job & pipeline definitions
.claude/skills/ # in-repo Claude Code skills (pipeline-docs, data-ingestion)
databricks.yml  # DAB targets (dev/prod)
profiles.yml    # dbt profiles
```

## Common commands

```bash
# Deploy bundle
databricks bundle deploy --target dev
databricks bundle deploy --target prod

# Run a job
databricks bundle run <job_name> --target dev

# dbt
dbt deps && dbt run && dbt test
```

See [CLAUDE.md](CLAUDE.md) for the full conventions (clustering, gold-SQL-only, JSON ingestion rules).
