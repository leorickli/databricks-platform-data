# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import json
from datetime import datetime, timedelta, timezone

import requests

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "land", "Schema Name for Volume")
dbutils.widgets.text("volume_name", "smartnode_batch", "Volume Name")
dbutils.widgets.text("offset_days", "1", "Offset in days for the most recent date to fetch (0=today, 1=yesterday)")
dbutils.widgets.text("lookback_days", "7", "How many consecutive days to fetch, ending at offset_days")
dbutils.widgets.text("secret_scope", "acme_smartnode_api_creds", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
OFFSET_DAYS = int(dbutils.widgets.get("offset_days"))
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Load Credentials
# base_url: e.g. "https://smartnode.eu/login/api/v1"
# api_key:  long-lived Apikey from Smartnode. Used only to mint a Bearer token;
#           never sent on data calls. Never logged.
BASE_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key="base_url").rstrip("/")
_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="api_key")

# COMMAND ----------

# DBTITLE 1,Authenticate — mint Bearer token
def _get_bearer_token(base_url: str, api_key: str) -> str:
    resp = requests.post(
        f"{base_url}/authenticate/bearer",
        headers={"Authorization": f"Apikey {api_key}", "Accept": "application/json"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise Exception(f"Smartnode auth failed: HTTP {resp.status_code} - {resp.text[:200]}")
    return resp.json()["token"]


def _api_get(base_url: str, token: str, path: str, params: dict | None = None) -> dict:
    resp = requests.get(
        f"{base_url}{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params=params,
        timeout=120,
    )
    if resp.status_code != 200:
        raise Exception(f"GET {path} failed: HTTP {resp.status_code} - {resp.text[:300]}")
    return resp.json()


_token = _get_bearer_token(BASE_URL, _API_KEY)

# COMMAND ----------

# DBTITLE 1,Create Landing Volume (if not exists)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON snapshots from the Smartnode mySmartnode REST API (data + metadata).'
""")

# COMMAND ----------

# DBTITLE 1,Calculate Target UTC Dates
# Lookback window: re-fetch the last LOOKBACK_DAYS days (ending at OFFSET_DAYS).
# Tolerates sensor downtime / late backfills: any day a sensor was offline gets
# re-asked on every subsequent run. Files don't collide (filename has processing_ts);
# silver is a full-recompute materialized view so re-ingested rows dedup naturally.
most_recent_date = (datetime.now(timezone.utc) - timedelta(days=OFFSET_DAYS)).date()
target_dates = [most_recent_date - timedelta(days=i) for i in range(LOOKBACK_DAYS)]
target_dates.sort()  # oldest → newest

print(f"Lookback    : {LOOKBACK_DAYS} day(s)")
print(f"Date range  : {target_dates[0]:%Y-%m-%d} → {target_dates[-1]:%Y-%m-%d} (UTC, inclusive)")

# COMMAND ----------

# DBTITLE 1,Discover accounts
# /accounts/ returns every account our API key can see. Today this is one
# (ACME demo) but the loop handles N>1 transparently if Smartnode provisions more.
accounts_resp = _api_get(BASE_URL, _token, "/accounts/")
accounts = accounts_resp.get("accounts", [])
account_ids = sorted(a["account"] for a in accounts)
if not account_ids:
    raise Exception("No accounts visible to this API key — nothing to fetch.")

print(f"Accounts visible: {len(account_ids)} → {account_ids}")

# COMMAND ----------

# DBTITLE 1,Fetch hourly energyassetbundles per account
# ignore_bundles=true returns ALL energyassetcategories the account has data for
# (not just those wired to a configured bundle). We fetch broad here; silver
# filters to the three electricity categories (10/26/27).
print(f"\n{'='*80}")
print(f"SMARTNODE DATA INGESTION — {target_dates[0]:%Y-%m-%d} → {target_dates[-1]:%Y-%m-%d} (UTC)")
print(f"{'='*80}")

results = []
total_files_written = 0
total_errors = 0

for account_id in account_ids:
    print(f"\n[Account {account_id}]")
    for target_date in target_dates:
        range_from = target_date.strftime("%Y-%m-%d")
        range_to = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
        path = f"/accounts/{account_id}/energyassetbundles/hour/{range_from}/{range_to}/"

        result = {
            "account_id": account_id,
            "target_date": range_from,
            "status": "pending",
            "file_name": None,
            "error": None,
            "row_count": 0,
        }

        try:
            raw_response = _api_get(BASE_URL, _token, path, params={"ignore_bundles": "true"})

            # Folder layout: {VOLUME_PATH}/{account_id}/data/{yyyymmdd}/
            folder_path = (
                f"{VOLUME_PATH}/{account_id}/data/{target_date.strftime('%Y%m%d')}"
            )
            dbutils.fs.mkdirs(folder_path)

            processing_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            file_name = (
                f"smartnode_data_{account_id}_{target_date.strftime('%Y%m%d')}"
                f"_{processing_ts}.json"
            )
            output_path = f"{folder_path}/{file_name}"

            envelope = {
                "account_id": account_id,
                "target_date": range_from,
                "range_from": range_from,
                "range_to": range_to,
                "processing_timestamp": processing_ts,
                "raw_response": raw_response,
            }

            dbutils.fs.put(output_path, json.dumps(envelope), overwrite=False)

            row_count = len(raw_response.get("energyassetbundles", []))
            print(f"  [{range_from}] -> OK: {file_name} ({row_count} rows)")

            result["status"] = "success"
            result["file_name"] = file_name
            result["row_count"] = row_count
            total_files_written += 1

        except Exception as e:
            err = str(e)
            print(f"  [{range_from}] -> FAILED: {err}")
            result["status"] = "error"
            result["error"] = err
            total_errors += 1

        results.append(result)

# COMMAND ----------

# DBTITLE 1,Summary
print(f"\n{'='*80}")
print(f"INGESTION SUMMARY")
print(f"{'='*80}")
print(f"Date range          : {target_dates[0]:%Y-%m-%d} → {target_dates[-1]:%Y-%m-%d} (UTC)")
print(f"Accounts processed  : {len(account_ids)}")
print(f"Account-day fetches : {len(account_ids) * len(target_dates)}")
print(f"Files written       : {total_files_written}")
print(f"Errors              : {total_errors}")
print(f"{'='*80}")

for r in results:
    if r["status"] == "success":
        print(f"  [acct {r['account_id']} {r['target_date']}] OK    {r['file_name']}  ({r['row_count']} rows)")
    else:
        print(f"  [acct {r['account_id']} {r['target_date']}] ERROR {r['error']}")

if total_errors > 0:
    raise Exception(f"{total_errors} account-day fetch(es) failed — see log above.")
