# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**lmx** is a reference data platform built on Databricks with both batch and streaming data processing, using a medallion architecture (land/bronze/silver/gold). It ingests data from pluggable connectors, transforms it, and exposes the resulting data (gold/silver tables) and connection metadata (the `connection_status` table populated by `src/shared/b10_update_batch_connection_metadata.py` and `src/shared/b11_update_streaming_connection_metadata.py`) to a frontend application.

Two example connectors ship in this repo — **ampcore** and **smartnode** — both built as Lakeflow Declarative Pipelines (LDP). They demonstrate the architecture and conventions; new connectors are added alongside them under `src/connectors/`.

### How `connection_status` is served to the frontend
`b10` (batch connectors) and `b11` (streaming) run as the final task of each pipeline and **append** one quality-metrics row per connection per run to `<client>.metadata.connection_status` (e.g. `acme_prod.metadata.connection_status`). This Delta table is the **source of truth**.

For low-latency serving, that table is **mirrored into Lakebase (managed Postgres)** via a **synced table** so the frontend gets sub-second reads without cold-starting a SQL warehouse (~7s). The synced table is a `FOREIGN`/`POSTGRESQL_FORMAT` table registered in the client catalog's `serving` schema (e.g. `acme_prod.serving.connection_status`, full-history mirror, PK `(connection_id, checked_at)`, `hourly_metrics` ARRAY<STRUCT> → JSONB). The frontend queries Postgres directly, authenticating as the `lmx_webapp_sp` service principal (Databricks OAuth token as the Postgres password).

All Lakebase infrastructure — the `lmx-serving` Autoscaling project, per-client Postgres databases, and the synced tables — is **Terraform-only**, in **lmx-infra** (`modules/databricks_workspace_production/lakebase.tf`). Do not create these from this repo. Enabling Change Data Feed on a source table (`ALTER TABLE … SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`) is required before a TRIGGERED synced table can mirror it.

## Common Commands

```bash
# Deploy to dev
databricks bundle deploy --target lmx-dev

# Deploy to production
databricks bundle deploy --target lmx-prod

# Run a specific job
databricks bundle run <job_name> --target lmx-dev
```

**Note:** the Databricks bundle targets and CLI profiles use the `lmx-dev` / `lmx-stg` / `lmx-prod` naming.

## Architecture

### Data Flow Architecture
The project follows a medallion architecture with these layers:
- **Land**: Raw payloads in UC volumes (file-based data)
- **Bronze**: Raw structured Delta tables
- **Silver**: Cleaned and enriched tables
- **Gold**: Business-ready, frontend-facing tables (SQL only)

### Source Structure
```
src/
├── connectors/       # One folder per data connector
│   ├── ampcore/
│   │   └── batch/    # LDP: *_api2land → *_land2bronze → *_bronze2silver → *_silver2gold (+ metadata pipeline)
│   └── smartnode/
│       └── batch/
├── shared/           # Shared cross-connector tasks
│   ├── b00_pause_controller.py
│   ├── b10_update_batch_connection_metadata.py    # Batch metadata → connection_status
│   ├── b11_update_streaming_connection_metadata.py
│   ├── b99_pipeline_failure_status.py
│   └── connection_status_utils.py   # incl. ensure_metadata_tables() — single source of the batch metadata DDL
├── clients/          # Client-specific helpers
└── lmx/              # Platform-wide utilities
    └── connector_volumes_cleanup.py
```

Each connector is a Lakeflow Declarative Pipeline: `*_data_land2bronze` → `*_data_bronze2silver` → `*_data_silver2gold.sql`, with a parallel `*_metadata_*` chain, then a `*_connection_config` task that seeds `metadata.connection_config`. The shared `b10`/`b11` tasks aggregate per-connector configs and write the `connection_status` records consumed by the frontend.

### Pipeline Types

#### Batch Connectors
- **ampcore**: Periodic REST API polling for gateway/sensor timeseries, landed as JSON then materialised through the LDP medallion layers.
- **smartnode**: Hourly REST API polling for device data + a daily metadata snapshot, materialised through the LDP medallion layers.
- **Data Flow**: API → Land Volume → Bronze Table → Silver Table → Gold

#### Stream Connectors
- The architecture supports Structured Streaming connectors (Kafka / Kinesis → Bronze → Silver → Gold) via `src/connectors/<name>/stream/`. No streaming connector ships in this reference repo.

### Key Components

#### Databricks Jobs Configuration
- Defined in `resources/*.yml`
- Uses Databricks Asset Bundles (DABs) for deployment
- Pipelines defined in `resources/acme_pipelines.yml`; jobs in `resources/acme_jobs.yml`

#### Environment Management
- `databricks.yml`: targets and variables for `lmx-dev` / `lmx-stg` / `lmx-prod`
- Separate catalogs per client/env: `acme_dev/prod`, `lmx_dev/prod`

### Development Workflow

1. **Local Development**: Test individual notebooks in the Databricks workspace
2. **Bundle Deployment**: Deploy jobs using `databricks bundle deploy`
3. **Monitoring**: Failure-status tasks alert on pipeline failures

### Conventions

#### Schemas and catalogs — never create new ones from this repo
Catalogs and schemas in Unity Catalog are provisioned exclusively by Terraform in the **lmx-infra** repo. **Never** issue `CREATE CATALOG`, `CREATE SCHEMA`, or `CREATE SCHEMA IF NOT EXISTS` from notebooks, jobs, or DDL files in this repo — even with `IF NOT EXISTS`. If a table needs to live somewhere that doesn't have a matching schema, **stop and ask** for the Terraform change before writing code that depends on it.

The schemas that exist in every client catalog (`acme_dev/prod`, `lmx_dev/prod`) are:
`land`, `bronze`, `silver`, `gold`, `views`, `metadata`, `operational`, `ml`, `checkpoints`, `serving`.

Pick the closest fit:
- `metadata` — `connection_config`, `connection_status`, availability/monitoring tables, and anything else describing connections or the running platform. Current default for new monitoring tables until we have a strong reason to split into `operational`.
- `views` — non-materialised views layered on top of `gold` (e.g. `views.ampcore_15min`)
- `bronze`/`silver`/`gold` — the medallion data layers proper
- `checkpoints` — Structured Streaming checkpoints
- `ml` — model artefacts and feature tables
- `land` — raw landed files (volumes, not Delta tables)
- `operational` — exists but currently unused; do not put new tables here without asking
- `serving` — **Lakebase serving layer.** Holds `FOREIGN` (`POSTGRESQL_FORMAT`) synced tables that mirror Delta tables into Lakebase Postgres for sub-second, no-warehouse reads by the frontend (e.g. `acme_prod.serving.connection_status`). These are **not** created from this repo — the Lakebase project, Postgres database, and synced tables are all Terraform (`databricks_postgres_*`) in **lmx-infra** (`modules/databricks_workspace_production/lakebase.tf`). See the Project Overview for the serving path.

Why: a `CREATE SCHEMA IF NOT EXISTS` from a notebook silently bypasses Terraform, which means the schema has no grants, no owner, no `comment`, and no record in IaC. It works in dev and then surprises us in prod where permissions are stricter. Creating tables is fine; creating the *namespace* they live in is an infra change.

#### Table partitioning — always liquid clustering with AUTO
Whenever a Delta / LDP table needs partitioning or clustering, use **liquid clustering with AUTO mode** by default — never Hive-style `partition_cols`, never explicit `cluster_by` columns unless there's a specific reason.

- LDP / `@dp.table`: set `cluster_by_auto=True`
- Plain Delta DDL: `CREATE TABLE ... CLUSTER BY AUTO` (or `ALTER TABLE ... CLUSTER BY AUTO` to convert)

Why: `CLUSTER BY AUTO` is the Databricks-recommended default. Predictive optimization observes the workload and picks clustering keys automatically, re-evaluating as query patterns change. No upfront commitment to which columns will be hot, no small-file fragmentation, and clustering keys can be swapped later without rewriting the table — none of which Hive partitioning gives you.

If you think a table needs explicit clustering keys, push back and explain why before falling back to non-AUTO. Hive partitioning (`partition_cols=`) is effectively never the right choice for new tables on this platform.

#### Gold layer — always SQL
All gold layer transformations must be written as SQL notebooks (`.sql`), never Python. This applies whether the source is silver (`silver2gold`) or bronze (`bronze2gold`) — if the logic can be expressed in SQL, it must be. Use `CREATE OR REFRESH MATERIALIZED VIEW` or `CREATE OR REFRESH STREAMING TABLE` as appropriate. Reserve Python (`@dp.table`) for bronze and silver layers where complex PySpark logic is needed (schema parsing, stateful streaming, pivots, UDFs). If you find yourself reaching for Python at gold, push back and find the SQL equivalent.

#### JSON ingestion — always explicit schema
When reading JSON with Autoloader (`cloudFiles`), `spark.read`, or inside an SDP/LDP pipeline function, always define an explicit `StructType` — never rely on schema inference. Required options: `cloudFiles.inferColumnTypes=false`, `cloudFiles.schemaEvolutionMode=none`, `cloudFiles.rescuedDataColumn=_rescued_data`. See `.claude/skills/data-ingestion/SKILL.md` for the full checklist and patterns.

### Key Files

- `databricks.yml`: Main configuration for environments and variables
- `resources/*.yml`: All Databricks job & pipeline definitions
- `src/connectors/*/batch/`: Batch processing notebooks per connector
- `src/connectors/*/stream/`: Streaming processing notebooks per connector
- `src/shared/`: Shared tasks (pause controller, metadata aggregation, failure status)
- `src/lmx/`: Platform-wide utilities (e.g., volume cleanup)
- `src/clients/`: Client-specific helpers

### Security
- Credentials stored in Databricks secrets scopes
- IAM roles for AWS resource access
- Separate service accounts for production
