# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Data Platform Exchange (DPX) data pipeline project built on Databricks with both batch and streaming data processing. The platform processes energy data from multiple clients (ACME and GLOBEX) using a medallion architecture (land/bronze/silver/gold).

The current project involves getting data from connectors (wattflow, tracksys, voltcore, ecosphere, sunpeak, solarflow, kafka_inverters), transforming them, and exposing their data (through the gold and/or silver tables) or metadata (through the `connection_status` table populated by `src/shared/b10_update_batch_connection_metadata.py` and `src/shared/b11_update_streaming_connection_metadata.py`) to a frontend application.

### How `connection_status` is served to the frontend
`b10` (batch connectors) and `b11` (streaming) run as the final task of each pipeline and **append** one quality-metrics row per connection per run to `<client>.metadata.connection_status` (per-client: `acme_prod.metadata.connection_status`, `globex_prod.metadata.connection_status`). This Delta table is the **source of truth**.

For low-latency serving, that table is **mirrored into Lakebase (managed Postgres)** via a **synced table** so the frontend gets sub-second reads without cold-starting a SQL warehouse (~7s). The synced table is a `FOREIGN`/`POSTGRESQL_FORMAT` table registered in the client catalog's `serving` schema (e.g. `acme_prod.serving.connection_status`, full-history mirror, PK `(connection_id, checked_at)`, `hourly_metrics` ARRAY<STRUCT> → JSONB). The frontend queries Postgres directly, authenticating as the `dpx_webapp_sp` service principal (Databricks OAuth token as the Postgres password).

All Lakebase infrastructure — the `dpx-serving` Autoscaling project, per-client Postgres databases, and the synced tables — is **Terraform-only**, in **dataplatformx-infra** (`modules/databricks_workspace_production/lakebase.tf`). Do not create these from this repo. Enabling Change Data Feed on a source table (`ALTER TABLE … SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`) is required before a TRIGGERED synced table can mirror it.

## Common Commands

### dbt Commands
```bash
# Run dbt models (from dbt/ directory)
dbt run

# Run dbt tests
dbt test

# Install dbt dependencies
dbt deps

# Run specific models
dbt run --select tracksys_batch --vars '{"acme_catalog":"acme_dev"}'
dbt run --select voltcore_batch --vars '{"globex_catalog":"globex_dev"}'
```

### Databricks Commands
```bash
# Deploy to dev environment
databricks bundle deploy --target dpx-dev

# Deploy to production
databricks bundle deploy --target dpx-prod

# Run a specific job
databricks bundle run <job_name> --target dpx-dev
```

**Note:** the Databricks bundle targets and CLI profiles were renamed from `dev`/`prod` to `dpx-dev`/`dpx-prod` (2026-05-29). dbt profiles in `profiles.yml` (`dataplatform_uc_dev` / `dataplatform_uc_prod`) were **not** renamed and stay as-is.

## Architecture

### Data Flow Architecture
The project follows a medallion architecture with these layers:
- **Land**: Raw data storage (volumes for file-based data)
- **Bronze**: Raw structured data tables
- **Silver**: Cleaned and enriched data tables
- **Gold**: Business-ready aggregated data (dbt models)

### Source Structure
```
src/
├── connectors/       # One folder per data connector
│   ├── ecosphere/
│   │   └── batch/    # b01_api2land → b02_land2bronze → b03_bronze2silver → b04_silver2gold → b05_connection_config
│   ├── solarflow/
│   │   └── batch/
│   ├── wattflow/
│   │   └── batch/
│   ├── kafka_inverters/
│   │   └── stream/   # Kafka streaming pipeline
│   ├── tracksys/
│   │   ├── batch/    # Legacy (no longer scheduled, historical data preserved)
│   │   └── stream/   # Kinesis streaming (DDL)
│   ├── sunpeak/
│   │   └── batch/
│   └── voltcore/
│       └── batch/
├── shared/           # Shared cross-connector tasks
│   ├── b00_pause_controller.py
│   ├── b10_update_batch_connection_metadata.py    # Batch metadata → connection_status
│   ├── b11_update_streaming_connection_metadata.py
│   ├── b99_pipeline_failure_status.py
│   └── connection_status_utils.py   # incl. ensure_metadata_tables() — single source of the batch metadata DDL
├── clients/          # Client-specific helpers (ACME, GLOBEX)
│   └── globex/globex_sites_and_spaces_metadata.py
└── dpx/              # DPX-wide utilities
    └── connector_volumes_cleanup.py
```

Per-connector notebooks follow a consistent numbered convention: `b01_api2land` → `b02_land2bronze` → `b03_bronze2silver` → `b04_silver2gold` (dbt), then `b05_connection_config` for the metadata pipeline. The shared `b10`/`b11` tasks aggregate per-connector configs and write the `connection_status` records consumed by the frontend.

### Pipeline Types

#### Batch Connectors
- **Wattflow**: Daily processing of construction-site DAP meter data from the Wattflow e-Dataportal API (Haystack-aligned)
- **Ecosphere**: Daily processing of building energy management data from the Ecosphere API (ESDL-aligned)
- **Voltcore / Sunpeak / Solarflow**: Periodic API polling for inverter and solar telemetry
- **TRACKSYS** (legacy): Historical CSV/ASC vehicle telemetry; no longer scheduled. **Excluded from active maintenance** — do not include tracksys (batch or stream) in ontology refactors, schema migrations, code-quality sweeps, or new feature work. Tables stay readable for historical lookups; the connector code is effectively frozen.
- **Data Flow**: API → Land Volume → Bronze Table → Silver Table → Gold (dbt)

#### Stream Connectors
- **Kafka Inverters (GLOBEX)**: Real-time processing from Kafka inverters topic
- **TRACKSYS (ACME)**: Real-time processing from AWS Kinesis (DDL only under `src/connectors/tracksys/stream/`)
- **Data Flow**: Kafka/Kinesis → Bronze Table → Silver Table → Gold (dbt)

### Key Components

#### Databricks Jobs Configuration
- Defined in `resources/jobs.yml`
- Uses Databricks Asset Bundle for deployment
- Cluster configurations optimized per workload (memory vs compute intensive)

#### dbt Models
- Located in `dbt/models/`
- Separate catalogs per client and environment
- Incremental materialization for gold layer tables

#### Environment Management
- `databricks.yml`: Environment-specific configurations
- `profiles.yml`: dbt connection profiles
- Separate catalogs: `acme_dev/prod`, `globex_dev/prod`, `dpx_dev/prod`

### Development Workflow

1. **Local Development**: Test individual notebooks in Databricks workspace
2. **Bundle Deployment**: Deploy jobs using `databricks bundle deploy`
3. **dbt Development**: Test dbt models locally with appropriate profile
4. **Monitoring**: Stream monitoring jobs alert on data pipeline failures

### Conventions

#### Schemas and catalogs — never create new ones from this repo
Catalogs and schemas in Unity Catalog are provisioned exclusively by Terraform in the **dataplatformx-infra** repo. **Never** issue `CREATE CATALOG`, `CREATE SCHEMA`, or `CREATE SCHEMA IF NOT EXISTS` from notebooks, dbt models, jobs, or DDL files in this repo — even with `IF NOT EXISTS`. If a table needs to live somewhere that doesn't have a matching schema, **stop and ask** for the Terraform change before writing code that depends on it.

The schemas that exist in every client catalog (`acme_dev/prod`, `globex_dev/prod`, `dpx_dev/prod`) are:
`land`, `bronze`, `silver`, `gold`, `views`, `metadata`, `operational`, `ml`, `checkpoints`, `serving`.

Pick the closest fit:
- `metadata` — `connection_config`, `connection_status`, availability/monitoring tables, and anything else describing connections or the running platform. Current default for new monitoring tables until we have a strong reason to split into `operational`.
- `views` — non-materialised views layered on top of `gold` (e.g. `views.ampcore_15min`)
- `bronze`/`silver`/`gold` — the medallion data layers proper
- `checkpoints` — Structured Streaming checkpoints
- `ml` — model artefacts and feature tables
- `land` — raw landed files (volumes, not Delta tables)
- `operational` — exists but currently unused; do not put new tables here without asking
- `serving` — **Lakebase serving layer.** Holds `FOREIGN` (`POSTGRESQL_FORMAT`) synced tables that mirror Delta tables into Lakebase Postgres for sub-second, no-warehouse reads by the frontend (e.g. `acme_prod.serving.connection_status`). These are **not** created from this repo — the Lakebase project, Postgres database, and synced tables are all Terraform (`databricks_postgres_*`) in **dataplatformx-infra** (`modules/databricks_workspace_production/lakebase.tf`). See the Project Overview for the serving path.

Why: a `CREATE SCHEMA IF NOT EXISTS` from a notebook silently bypasses Terraform, which means the schema has no grants, no owner, no `comment`, and no record in IaC. It works in dev and then surprises us in prod where permissions are stricter. Creating tables is fine; creating the *namespace* they live in is an infra change.

#### Table partitioning — always liquid clustering with AUTO
Whenever a Delta / LDP table needs partitioning or clustering, use **liquid clustering with AUTO mode** by default — never Hive-style `partition_cols`, never explicit `cluster_by` columns unless there's a specific reason.

- LDP / `@dp.table`: set `cluster_by_auto=True`
- Plain Delta DDL: `CREATE TABLE ... CLUSTER BY AUTO` (or `ALTER TABLE ... CLUSTER BY AUTO` to convert)
- dbt models: `cluster_by='auto'` in the model config

Why: `CLUSTER BY AUTO` is the Databricks-recommended default. Predictive optimization observes the workload and picks clustering keys automatically, re-evaluating as query patterns change. No upfront commitment to which columns will be hot, no small-file fragmentation, and clustering keys can be swapped later without rewriting the table — none of which Hive partitioning gives you.

If you think a table needs explicit clustering keys, push back and explain why before falling back to non-AUTO. Hive partitioning (`partition_cols=`) is effectively never the right choice for new tables on this platform.

#### Gold layer — always SQL
All gold layer transformations must be written as SQL notebooks (`.sql`), never Python. This applies whether the source is silver (`silver2gold`) or bronze (`bronze2gold`) — if the logic can be expressed in SQL, it must be. Use `CREATE OR REFRESH MATERIALIZED VIEW` or `CREATE OR REFRESH STREAMING TABLE` as appropriate. Reserve Python (`@dp.table`) for bronze and silver layers where complex PySpark logic is needed (schema parsing, stateful streaming, pivots, UDFs). If you find yourself reaching for Python at gold, push back and find the SQL equivalent.

#### JSON ingestion — always explicit schema
When reading JSON with Autoloader (`cloudFiles`), `spark.read`, or inside an SDP/LDP pipeline function, always define an explicit `StructType` — never rely on schema inference. Required options: `cloudFiles.inferColumnTypes=false`, `cloudFiles.schemaEvolutionMode=none`, `cloudFiles.rescuedDataColumn=_rescued_data`. See `.claude/skills/data-ingestion/SKILL.md` for the full checklist and patterns.

### Key Files

- `databricks.yml`: Main configuration for environments and variables
- `resources/jobs.yml`: All Databricks job definitions
- `profiles.yml`: dbt connection configurations
- `src/connectors/*/batch/`: Batch processing notebooks per connector
- `src/connectors/*/stream/`: Streaming processing notebooks per connector
- `src/shared/`: Shared tasks (pause controller, metadata aggregation, failure status)
- `src/dpx/`: DPX-wide utilities (e.g., volume cleanup)
- `src/clients/`: Client-specific helpers (ACME, GLOBEX)
- `.claude/skills/pipeline-docs/`: Documentation skill + canonical template for writing connector docs

### Data Sources
- **Wattflow**: Wattflow e-Dataportal API (JSON — construction-site DAP meters)
- **Ecosphere**: Ecosphere API at `api.ecosphere.nl` (JSON — building energy management)
- **Voltcore**: Voltcore API (inverter telemetry)
- **Sunpeak**: Sunpeak API (solar telemetry)
- **Solarflow**: Solarflow API (solar/inverter telemetry)
- **TRACKSYS** (legacy): TRACKSYS Solutions API (CSV/ASC files, base64-encoded)
- **Kafka inverters (GLOBEX)**: Kafka `inverters` topic
- **Kinesis (ACME)**: AWS Kinesis streams

### Security
- Credentials stored in Databricks secrets scopes
- IAM roles for AWS resource access
- Separate service accounts for production