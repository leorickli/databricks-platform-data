# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import json
import urllib3
from datetime import datetime

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
dbutils.widgets.text("secret_scope", "acme_ampcore_api_creds", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Load Credentials
# base_url: full scheme+host+port, e.g. "https://46.244.5.43:55558"
# token:    static REST API token from SCU200 "System setup > Communication > Rest API"
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

# DBTITLE 1,Fetch Metadata Snapshot
url = f"{BASE_URL}/api/v1/metadata"
print(f"GET {url}")

response = requests.get(url, headers=HEADERS, verify=False, timeout=120)

if response.status_code != 200:
    raise Exception(f"Metadata fetch failed: HTTP {response.status_code} - {response.text[:500]}")

metadata = response.json()

gateway_id = metadata.get("id", "unknown")
gateway_ip = metadata.get("ip", "unknown")
gateway_serial = metadata.get("serialNumber", "unknown")

print(f"Gateway: id={gateway_id} ip={gateway_ip} serial={gateway_serial}")
print(f"Device types returned: {[k for k, v in metadata.items() if isinstance(v, list)]}")

# COMMAND ----------

# DBTITLE 1,Write Snapshot to Landing Volume
# Folder layout: {VOLUME_PATH}/{gateway_id}/metadata/{yyyymmdd}/
# One snapshot per day is enough — the metadata endpoint returns the
# current inventory state, not a time series.
snapshot_date = datetime.utcnow().strftime("%Y%m%d")
processing_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

folder_path = f"{VOLUME_PATH}/{gateway_id}/metadata/{snapshot_date}"
dbutils.fs.mkdirs(folder_path)

file_name = f"ampcore_metadata_{gateway_id}_{processing_ts}.json"
output_path = f"{folder_path}/{file_name}"

envelope = {
    "snapshot_date": snapshot_date,
    "processing_timestamp": processing_ts,
    "raw_response": metadata,
}

dbutils.fs.put(output_path, json.dumps(envelope, indent=2), overwrite=False)
print(f"Written: {output_path}")

# COMMAND ----------

# DBTITLE 1,Summary
current_sensors = metadata.get("CurrentSensor", [])
print(f"\n{'='*80}")
print(f"AMPCORE METADATA INGESTION SUMMARY")
print(f"{'='*80}")
print(f"Gateway           : {gateway_id} ({gateway_ip})")
print(f"Serial            : {gateway_serial}")
print(f"CurrentSensors    : {len(current_sensors)}")
print(f"Output file       : {file_name}")
print(f"{'='*80}")
