# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import json
import urllib3
from datetime import datetime, timedelta, timezone

import requests

# The SCU200 gateway ships with a self-signed TLS certificate.
# We intentionally disable verification for this connector only;
# silence the resulting urllib3 warning so job logs stay clean.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "land", "Schema Name for Volume")
dbutils.widgets.text("volume_name", "ampcore_batch", "Volume Name")
dbutils.widgets.text("offset_days", "1", "Offset in days for the most recent date to fetch (0=today, 1=yesterday)")
dbutils.widgets.text("lookback_days", "7", "How many consecutive days to fetch, ending at offset_days")
dbutils.widgets.text("secret_scope", "acme_ampcore_api_creds", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
OFFSET_DAYS = int(dbutils.widgets.get("offset_days"))
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# Only CurrentSensor devices are in scope — see template.md AMPCORE section.
# Every CurrentSensor exposes the same 5 variables, so we reuse this list for all 23 sensors.
DEVICE_TYPE = "CurrentSensor"
VARIABLES = [
    "currentTrms",
    "currentAc",
    "currentDc",
    "activePowerTotal",
    "activeEnergyTotal",
]

# 30s resolution × 8h window = 960 samples per series, under the 1000-sample API cap.
# Three 8h windows cover a full UTC day.
RESOLUTION = "30s"
WINDOW_HOURS = 8
WINDOWS_PER_DAY = 24 // WINDOW_HOURS  # 3

# COMMAND ----------

# DBTITLE 1,Load Credentials
BASE_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key="base_url")
API_TOKEN = dbutils.secrets.get(scope=SECRET_SCOPE, key="authorization_token")

HEADERS = {
    "Authorization": API_TOKEN,
    "Content-Type": "application/json",
}

# COMMAND ----------

# DBTITLE 1,Create Landing Volume (if not exists)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON snapshots from the AMPCORE SCU200 REST API (metadata + data endpoints).'
""")

# COMMAND ----------

# DBTITLE 1,Calculate Target Dates and Time Windows
# Lookback window: re-fetch the last LOOKBACK_DAYS days (ending at OFFSET_DAYS).
# This makes the job tolerant to sensor downtime / API outages / late-arriving
# backfills: any day a sensor was offline gets re-asked on every subsequent run.
# Re-fetched files do not collide because the filename includes processing_ts;
# silver dedups via stateful streaming on (gateway_id, object_id, timestamp).
most_recent_date = (datetime.now(timezone.utc) - timedelta(days=OFFSET_DAYS)).date()
target_dates = [most_recent_date - timedelta(days=i) for i in range(LOOKBACK_DAYS)]
target_dates.sort()  # oldest → newest

def build_windows(d):
    day_start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    out = []
    for w in range(WINDOWS_PER_DAY):
        begin = day_start + timedelta(hours=w * WINDOW_HOURS)
        end = begin + timedelta(hours=WINDOW_HOURS)
        out.append({
            "index": w,
            "begin_dt": begin,
            "end_dt": end,
            "begin_ts": int(begin.timestamp()),
            # end is exclusive on our side; the API treats begin/end inclusively so we
            # subtract one second to avoid overlapping the next window's first sample.
            "end_ts": int(end.timestamp()) - 1,
        })
    return out

print(f"Lookback     : {LOOKBACK_DAYS} day(s)")
print(f"Date range   : {target_dates[0]:%Y-%m-%d} → {target_dates[-1]:%Y-%m-%d} (UTC, inclusive)")
print(f"Resolution   : {RESOLUTION}")
print(f"Windows/day  : {WINDOWS_PER_DAY} × {WINDOW_HOURS}h")

# COMMAND ----------

# DBTITLE 1,Discover CurrentSensor object_ids from live metadata
# We re-fetch metadata here (rather than reading the land volume) so the data task
# is self-contained and always requests the current device inventory. Cheap call.
print(f"\nGET {BASE_URL}/api/v1/metadata")
meta_response = requests.get(
    f"{BASE_URL}/api/v1/metadata", headers=HEADERS, verify=False, timeout=120
)
if meta_response.status_code != 200:
    raise Exception(
        f"Metadata fetch failed: HTTP {meta_response.status_code} - {meta_response.text[:500]}"
    )
metadata = meta_response.json()

gateway_id = metadata.get("id", "unknown")
gateway_ip = metadata.get("ip", "unknown")
gateway_serial = metadata.get("serialNumber", "unknown")

sensors = metadata.get(DEVICE_TYPE, [])
object_ids = sorted(s["object_id"] for s in sensors)
if not object_ids:
    raise Exception(f"No {DEVICE_TYPE} devices returned from metadata — nothing to fetch.")

print(f"Gateway        : {gateway_id} ({gateway_ip}) / {gateway_serial}")
print(f"{DEVICE_TYPE}s  : {len(object_ids)} → {object_ids}")

# All sensors share the same variable list, so one map covers the whole batch.
values_map = {str(oid): VARIABLES for oid in object_ids}

# COMMAND ----------

# DBTITLE 1,Fetch Each Day × Each Time Window
print(f"\n{'='*80}")
print(f"AMPCORE DATA INGESTION — {target_dates[0]:%Y-%m-%d} → {target_dates[-1]:%Y-%m-%d} (UTC)")
print(f"{'='*80}")

results = []
total_files_written = 0
total_errors = 0

for target_date in target_dates:
    print(f"\n--- Day {target_date:%Y-%m-%d} ---")
    for w in build_windows(target_date):
        print(f"\n[{target_date:%Y-%m-%d} W{w['index'] + 1}/{WINDOWS_PER_DAY}] "
              f"{w['begin_dt']:%H:%M} → {w['end_dt']:%H:%M}")

        payload = {
            "data": [
                {
                    "type": "historical",
                    "values": values_map,
                    "begin_timestamp": w["begin_ts"],
                    "end_timestamp": w["end_ts"],
                    "resolution": RESOLUTION,
                    "consumption": False,
                }
            ]
        }

        result = {
            "target_date": target_date.strftime("%Y-%m-%d"),
            "window_index": w["index"],
            "begin_ts": w["begin_ts"],
            "end_ts": w["end_ts"],
            "status": "pending",
            "file_name": None,
            "error": None,
        }

        try:
            url = f"{BASE_URL}/api/v1/data"
            # 600s timeout: the SCU200 is a small embedded ARM device and can
            # take minutes to assemble large historical responses, especially
            # for older days that aren't cached. 120s is fine for yesterday but
            # fails on 5–7-day-old windows under the 7-day lookback.
            response = requests.post(
                url, headers=HEADERS, json=payload, verify=False, timeout=600
            )

            if response.status_code != 200:
                err = f"HTTP {response.status_code}: {response.text[:300]}"
                print(f"  -> ERROR {err}")
                result["status"] = "error"
                result["error"] = err
                total_errors += 1
                results.append(result)
                continue

            raw_response = response.json()

            # Folder layout: {VOLUME_PATH}/{gateway_id}/data/{yyyymmdd}/
            folder_path = (
                f"{VOLUME_PATH}/{gateway_id}/data/{target_date.strftime('%Y%m%d')}"
            )
            dbutils.fs.mkdirs(folder_path)

            processing_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            file_name = (
                f"ampcore_data_{gateway_id}_{target_date.strftime('%Y%m%d')}"
                f"_w{w['index']}_{processing_ts}.json"
            )
            output_path = f"{folder_path}/{file_name}"

            envelope = {
                "target_date": target_date.strftime("%Y-%m-%d"),
                "window_index": w["index"],
                "begin_timestamp": w["begin_ts"],
                "end_timestamp": w["end_ts"],
                "resolution": RESOLUTION,
                "processing_timestamp": processing_ts,
                "raw_response": raw_response,
            }

            dbutils.fs.put(output_path, json.dumps(envelope), overwrite=False)

            # Quick record count for logging (raw_response["data"] is a list of samples).
            sample_count = len(raw_response.get("data", [])) if isinstance(raw_response, dict) else 0
            print(f"  -> OK: {file_name} ({sample_count} samples)")

            result["status"] = "success"
            result["file_name"] = file_name
            result["sample_count"] = sample_count
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
print(f"INGESTION SUMMARY")
print(f"{'='*80}")
print(f"Date range          : {target_dates[0]:%Y-%m-%d} → {target_dates[-1]:%Y-%m-%d} (UTC)")
print(f"Days × windows      : {len(target_dates)} × {WINDOWS_PER_DAY} = {len(target_dates) * WINDOWS_PER_DAY}")
print(f"Files written       : {total_files_written}")
print(f"Errors              : {total_errors}")
print(f"{'='*80}")

for r in results:
    status = r["status"]
    if status == "success":
        print(f"  [{r['target_date']} W{r['window_index']}] OK    {r['file_name']}  "
              f"({r.get('sample_count', '?')} samples)")
    else:
        print(f"  [{r['target_date']} W{r['window_index']}] ERROR {r.get('error', 'unknown')}")

if total_errors > 0:
    raise Exception(f"{total_errors} window(s) failed — see log above.")
