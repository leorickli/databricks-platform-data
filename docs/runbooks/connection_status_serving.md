# Runbook — `connection_status` serving (Delta → Lakebase → frontend)

How the frontend reads connection health, and the **exact steps to push a schema
change live** when you add/remove columns on `connection_status`.

This spans two repos:
- **lmx-data** (this repo) — produces the source table via `b10`/`b11`.
- **lmx-infra** — owns the Lakebase project, synced tables, and the
  refresh job (`modules/databricks_workspace_*/lakebase.tf`).

## Architecture (why a schema change is a multi-step migration)

```
b10/b11 append ──► <client>.metadata.connection_status   (Delta, source of truth)
                          │  (TRIGGERED sync, CDF-based, debounced 1h)
                          ▼
            <client>.serving.connection_status            (FOREIGN / POSTGRESQL_FORMAT
                          │                                synced table → Lakebase Postgres)
                          ▼
            frontend  ◄── reads Postgres directly as lmx_webapp_sp (OAuth token = pw)
```

Key facts that drive the runbook:
- The synced table is a **full-table mirror** — no column list in Terraform, so new
  source columns sync automatically. `ARRAY<STRUCT>` columns (`hourly_metrics`,
  `data_gaps`) land as **JSONB**.
- **Schema changes are NOT auto-propagated.** A TRIGGERED synced table keeps the
  Postgres schema it was *created* with. New columns only appear after the synced
  table is **recreated** (`terraform apply -replace`).
- The recreated backing table is owned by the managed writer role
  (`databricks_writer_24579`), **not** the Terraform SP — so default privileges
  can't cover it and the **webapp SELECT grant must be re-applied by hand** each time.
- New *rows* (every normal `b10` run) need none of this — the refresh job
  (`connection_status_sync`, `table_update` trigger, 1h debounce) handles them.

## The `data_gaps` metric (completeness subtask 3.4)

`b10` writes two columns the frontend timeline uses:
- `data_gap_count INT` — number of distinct gaps (contiguous runs of missing
  expected records) in last 24h. Measures *how missing data is distributed*,
  complementing `completeness_pct` (*how much* is missing).
- `data_gaps ARRAY<STRUCT<gap_start, gap_end, gap_duration_minutes, missing_records>>`
  — one entry per gap; powers the painted timeline segments.

`missing_data_pct` is **intentionally not stored** — derive it as
`100 - completeness_pct` on read. Detection logic:
`src/shared/connection_status_utils.py` → `calculate_data_gaps()`.

## Runbook — push a source schema change live

> Replace `<env>` placeholders with the values from the per-environment table below.

**0. Get the new columns into the SOURCE first.** The recreate snapshots whatever
schema exists at replace time, so this must happen before step 1. Either:
- run an explicit `ALTER TABLE <catalog>.metadata.connection_status ADD COLUMNS (...)`, or
- let one `b10` run add them via `mergeSchema=true` (the write schema always
  carries the new fields, so the first run evolves the table even with no gaps).

Run it on **every** client catalog so they don't drift (`acme_*`, dev/stg).

**1. Recreate the synced table** (re-snapshots full history with the new schema),
run from `lmx-infra/`:

```bash
# Production
terraform apply -replace='module.databricks_workspace_production.databricks_postgres_synced_table.acme_connection_status'

# Staging
terraform apply -replace='module.databricks_workspace_staging.databricks_postgres_synced_table.acme_connection_status'
```

**2. Re-grant SELECT to the webapp SP** (the recreate drops the out-of-band grant).
Connect to the client's `acme` database as a superuser role (see "Connecting" below),
then in psql:

```sql
-- (a) find the webapp SP role: the only UUID role that is NOT the Terraform SP
SELECT rolname FROM pg_roles
WHERE rolname ~ '^[0-9a-f]{8}-' AND rolname <> '11111111-1111-1111-1111-111111111111';

-- (b) grant — paste the UUID from (a), keep the double quotes
GRANT SELECT ON serving.connection_status TO "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";

-- (c) verify — expect a row:  "<uuid>"=r/databricks_writer_24579
\dp serving.connection_status
```

**3. (Optional) Force an immediate refresh** instead of waiting up to 1h for the
trigger: run the `lmx-serving[-stg] - acme.connection_status sync refresh` job from
the Workflows UI (Run now). The `-replace` already re-snapshotted everything that
existed at that moment; only rows appended *after* the replace wait on the trigger.

**4. Confirm the new columns + data landed:**

```sql
\d serving.connection_status                 -- data_gaps should be jsonb
SELECT connection_id, checked_at, data_gap_count, data_gaps
FROM serving.connection_status
WHERE data_gap_count > 0
ORDER BY checked_at DESC LIMIT 20;
```

## Connecting to Lakebase from psql

Everyday one-liner (fresh OAuth token each run — tokens expire hourly). Copy as-is:

**Production:**

```bash
PGPASSWORD=$(databricks auth token -p lmx-prod | jq -r .access_token) \
  psql "host=ep-example-dev-0000.database.eu-central-1.cloud.databricks.com dbname=acme user=developer@example.com sslmode=require"
```

**Staging:**

```bash
PGPASSWORD=$(databricks auth token -p lmx-stg | jq -r .access_token) \
  psql "host=ep-example-prod-0000.database.eu-central-1.cloud.databricks.com dbname=acme user=developer@example.com sslmode=require"
```

Notes:
- The Lakebase endpoint accepts your Databricks OAuth access token as the Postgres password.
- Do **not** wrap the host/profile in `< >` — zsh reads angle brackets as redirection
  (`zsh: parse error near '|'`). Paste the literal values above.
- If you paste the UI connection string instead, **quote it** so zsh doesn't glob the
  `?sslmode=require`: `psql "postgresql://...?sslmode=require"`.
- Endpoint hosts can change if the project is recreated — re-fetch with:
  `databricks postgres list-endpoints projects/<project>/branches/main -p <profile>`.

## Per-environment values

| | Production | Staging |
|---|---|---|
| Module (for `-replace`) | `databricks_workspace_production` | `databricks_workspace_staging` |
| Source table | `acme_prod.metadata.connection_status` | `acme_stg.metadata.connection_status` |
| Catalog | `acme_prod` | `acme_stg` |
| Lakebase project | `lmx-serving` | `lmx-serving-stg` |
| CLI profile | `lmx-prod` | `lmx-stg` |
| Endpoint host | `ep-example-dev-0000.database.eu-central-1.cloud.databricks.com` | `ep-example-prod-0000.database.eu-central-1.cloud.databricks.com` |

The Terraform SP (excluded in the `pg_roles` query above) is the same in both:
`11111111-1111-1111-1111-111111111111`. Only an **acme** synced table exists today —
there is no second-client synced table yet. Endpoint hosts are current as of 2026-06-16;
re-fetch with `databricks postgres list-endpoints projects/<project>/branches/main -p <profile>`
if a project is ever recreated.
