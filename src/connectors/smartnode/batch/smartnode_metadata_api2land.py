# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import json
from datetime import datetime, timezone

import requests

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "land", "Schema Name for Volume")
dbutils.widgets.text("volume_name", "smartnode_batch", "Volume Name")
dbutils.widgets.text("secret_scope", "acme_smartnode_api_creds", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Load Credentials
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

# DBTITLE 1,Fetch metadata snapshot per account
# One snapshot per day per account. The metadata endpoints are tiny (account info,
# address, energyasset list) so we capture the full 3-resource picture in one
# envelope file rather than scattering across three autoloaders downstream.
snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")
processing_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

print(f"\n{'='*80}")
print(f"SMARTNODE METADATA INGESTION — {snapshot_date} (UTC)")
print(f"{'='*80}")

# Fetch the account list once. Even if it returns one row today, the per-account
# loop below makes the pipeline N-account-ready with no code change.
accounts_resp = _api_get(BASE_URL, _token, "/accounts/")
accounts = accounts_resp.get("accounts", [])
if not accounts:
    raise Exception("No accounts visible to this API key — nothing to fetch.")

print(f"Accounts visible: {len(accounts)}")

results = []
total_files_written = 0
total_errors = 0

for acct in accounts:
    account_id = acct["account"]
    print(f"\n[Account {account_id}]")

    result = {
        "account_id": account_id,
        "status": "pending",
        "file_name": None,
        "error": None,
        "asset_count": 0,
    }

    try:
        addresses = _api_get(BASE_URL, _token, f"/accounts/{account_id}/addresses/").get("addresses", [])
        energyassets = _api_get(BASE_URL, _token, f"/accounts/{account_id}/energyassets/").get("energyassets", [])

        # Folder layout: {VOLUME_PATH}/{account_id}/metadata/{yyyymmdd}/
        folder_path = f"{VOLUME_PATH}/{account_id}/metadata/{snapshot_date}"
        dbutils.fs.mkdirs(folder_path)

        file_name = f"smartnode_metadata_{account_id}_{processing_ts}.json"
        output_path = f"{folder_path}/{file_name}"

        envelope = {
            "snapshot_date": snapshot_date,
            "processing_timestamp": processing_ts,
            "account_id": account_id,
            "raw_response": {
                "account": acct,
                "addresses": addresses,
                "energyassets": energyassets,
            },
        }

        dbutils.fs.put(output_path, json.dumps(envelope, indent=2), overwrite=False)

        asset_count = len(energyassets)
        print(f"  -> OK: {file_name} ({asset_count} asset(s), {len(addresses)} address(es))")

        result["status"] = "success"
        result["file_name"] = file_name
        result["asset_count"] = asset_count
        total_files_written += 1

    except Exception as e:
        err = str(e)
        print(f"  -> FAILED: {err}")
        result["status"] = "error"
        result["error"] = err
        total_errors += 1

    results.append(result)

# COMMAND ----------

# DBTITLE 1,Summary
print(f"\n{'='*80}")
print(f"SMARTNODE METADATA INGESTION SUMMARY")
print(f"{'='*80}")
print(f"Snapshot date       : {snapshot_date} (UTC)")
print(f"Accounts processed  : {len(accounts)}")
print(f"Files written       : {total_files_written}")
print(f"Errors              : {total_errors}")
print(f"{'='*80}")

for r in results:
    if r["status"] == "success":
        print(f"  [acct {r['account_id']}] OK    {r['file_name']}  ({r['asset_count']} asset(s))")
    else:
        print(f"  [acct {r['account_id']}] ERROR {r['error']}")

if total_errors > 0:
    raise Exception(f"{total_errors} account(s) failed — see log above.")
