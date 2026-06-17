# Databricks notebook source
# DBTITLE 1,Notebook Overview
"""
WATTFLOW INACTIVE EAN BACKFILL TO LAND LAYER
==========================================

Purpose:
This notebook backfills historical data for INACTIVE EANs by writing JSON files
to the acme_dev.land.wattflow_dap_batch volume. Once the data is in the volume,
the regular daily batch jobs (b02_wattflow_land2bronze.py and b03_wattflow_bronze2silver.py)
will automatically process it.

Why separate backfill for inactive EANs?
- Inactive EANs are no longer included in the daily active EAN list
- They have complete historical data that needs to be preserved
- By writing to the same land volume, we ensure consistent processing

Workflow:
1. Run this notebook to fetch and write inactive EAN data to land volume
2. Run b02_wattflow_land2bronze.py to process land → bronze
3. Run b03_wattflow_bronze2silver.py to process bronze → silver
4. dbt gold models will automatically include the data

Configuration:
- Use widgets to control which inactive EANs to backfill
- Configurable date ranges per EAN (or use full historical range)
- Writes data in same format as active pipeline for seamless integration
"""

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import requests
import json
import time
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "wattflow_dap_batch", "Volume Name")
dbutils.widgets.dropdown("backfill_mode", "all_inactive", ["all_inactive", "specific_eans"], "Backfill Mode")
dbutils.widgets.text("specific_eans", "", "Specific EANs (comma-separated)")
dbutils.widgets.text("chunk_days", "300", "Chunk Size (days)")
dbutils.widgets.dropdown("date_range_mode", "full_history", ["full_history", "custom_range"], "Date Range Mode")
dbutils.widgets.text("custom_start_date", "", "Custom Start Date (YYYY-MM-DD)")
dbutils.widgets.text("custom_end_date", "", "Custom End Date (YYYY-MM-DD)")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
BACKFILL_MODE = dbutils.widgets.get("backfill_mode")
SPECIFIC_EANS = dbutils.widgets.get("specific_eans")
CHUNK_DAYS = int(dbutils.widgets.get("chunk_days"))
DATE_RANGE_MODE = dbutils.widgets.get("date_range_mode")
CUSTOM_START_DATE = dbutils.widgets.get("custom_start_date")
CUSTOM_END_DATE = dbutils.widgets.get("custom_end_date")

# Get API credentials
API_KEY = dbutils.secrets.get(scope="acme_wattflow_api_creds", key="api_key")

# Configure paths
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

# API endpoints
BASE_URL = "https://wattflow.e-dataportal.nl/api/v3"
CONNECTIONS_URL = f"{BASE_URL}/connections/"
AGGREGATES_URL = f"{BASE_URL}/aggregates/"

print("=" * 80)
print("WATTFLOW INACTIVE EAN BACKFILL - CONFIGURATION")
print("=" * 80)
print(f"Catalog: {CATALOG_NAME}")
print(f"Volume Path: {VOLUME_PATH}")
print(f"Backfill Mode: {BACKFILL_MODE}")
print(f"Date Range Mode: {DATE_RANGE_MODE}")
print(f"Chunk Size: {CHUNK_DAYS} days")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Define Inactive EANs List
"""
These are EANs that are no longer active but have historical data.
Source: Wattflow EAN sleutels CSV file (Status: Inactief)
"""

ALL_INACTIVE_EANS = [
    {
        'ean': '871689260011746329',
        'location': 'Hazelaarweg 100 Bij',
        'city': 'Rotterdam',
        'entity': 'B&T',
        'sleutel': '56621452'
    },
    {
        'ean': '871685920004048543',
        'location': 'Koningin Wilhelminaplein 1',
        'city': 'Amsterdam',
        'entity': 'B&T Speciale Projecten',
        'sleutel': '59868668'
    },
    {
        'ean': '871687110000967926',
        'location': 'De Serpeling 120',
        'city': 'Lelystad',
        'entity': 'BES',
        'sleutel': '50624190'
    },
    {
        'ean': '871685900041044883',
        'location': 'Oosterengweg 38',
        'city': 'Unknown',
        'entity': 'Services Nederland',
        'sleutel': '56218582'
    },
    {
        'ean': '871687120000023324',
        'location': 'Plantijnweg 32',
        'city': 'Unknown',
        'entity': 'Services Nederland',
        'sleutel': '54077063'
    },
    {
        'ean': '871685920004378541',
        'location': 'Dijkmeerlaan 551',
        'city': 'Amsterdam',
        'entity': 'Wonen',
        'sleutel': '67235442'
    },
    {
        'ean': '871689260012516082',
        'location': 'de Eik 56 BIJ',
        'city': 'HELLEVOETSLUIS',
        'entity': 'Wonen Bouw op Maat',
        'sleutel': '63398280'
    },
    {
        'ean': '871688660012398397',
        'location': 'Ambachtsherenpad 8 BIJ',
        'city': 'ZOETERMEER',
        'entity': 'Wonen',
        'sleutel': '63087252'
    },
    {
        'ean': '871689260012250047',
        'location': '2e Rosestraat 11',
        'city': 'ROTTERDAM',
        'entity': 'Wonen',
        'sleutel': '67235473'
    },
    {
        'ean': '871689260012250054',
        'location': '2e Rosestraat 7',
        'city': 'ROTTERDAM',
        'entity': 'Wonen',
        'sleutel': '67235472'
    }
]

# Determine which EANs to process
if BACKFILL_MODE == "specific_eans" and SPECIFIC_EANS:
    specific_ean_list = [ean.strip() for ean in SPECIFIC_EANS.split(",")]
    eans_to_process = [ean_info for ean_info in ALL_INACTIVE_EANS if ean_info['ean'] in specific_ean_list]
    print(f"\nProcessing {len(eans_to_process)} specific EAN(s): {specific_ean_list}")
else:
    eans_to_process = ALL_INACTIVE_EANS
    print(f"\nProcessing ALL {len(eans_to_process)} inactive EANs")

if not eans_to_process:
    raise ValueError("No EANs to process. Check your configuration.")

print("\nEANs to backfill:")
for idx, ean_info in enumerate(eans_to_process, 1):
    print(f"  {idx}. {ean_info['ean']} - {ean_info['location']}, {ean_info['city']} ({ean_info['entity']})")

# COMMAND ----------

# DBTITLE 1,Verify Landing Volume Exists
print("\n" + "=" * 80)
print("VERIFYING INFRASTRUCTURE")
print("=" * 80)

# Create landing volume if not exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON aggregates from the Wattflow API at various granularities.'
""")
print(f"✓ Volume verified: {CATALOG_NAME}.land.{VOLUME_NAME}")

# COMMAND ----------

# DBTITLE 1,Backfill Data to Land Volume
print("\n" + "=" * 80)
print("BACKFILLING INACTIVE EAN DATA TO LAND VOLUME")
print("=" * 80)

# Track statistics
results = []
total_files_written = 0
total_api_calls = 0
total_days_covered = 0

for idx, ean_info in enumerate(eans_to_process, 1):
    ean = ean_info['ean']
    print(f"\n{'='*80}")
    print(f"[{idx}/{len(eans_to_process)}] Processing EAN: {ean}")
    print(f"Location: {ean_info['location']}, {ean_info['city']}")
    print(f"Entity: {ean_info['entity']}")
    print(f"{'='*80}")

    result = {
        'ean': ean,
        'location': ean_info['location'],
        'city': ean_info['city'],
        'entity': ean_info['entity'],
        'status': 'pending',
        'dap_id': None,
        'aggregation_level': None,
        'first_measurement': None,
        'last_measurement': None,
        'total_days': 0,
        'files_written': 0,
        'api_calls': 0,
        'error': None
    }

    try:
        # Step 1: Get metadata for this EAN
        print("→ Fetching metadata from Wattflow API...")
        params = {
            'apikey': API_KEY,
            'ean': ean,
            'format': 'json'
        }

        response = requests.get(CONNECTIONS_URL, params=params)
        response.raise_for_status()

        metadata = response.json()['values'][0]
        dap_id = str(metadata['id'])
        first_measurement_str = metadata['first-measurement']
        last_measurement_str = metadata['last-measurement']
        lowest_agg_level = metadata.get('lowest_aggregation_level', 'daily')

        result['dap_id'] = dap_id
        result['aggregation_level'] = lowest_agg_level
        result['first_measurement'] = first_measurement_str
        result['last_measurement'] = last_measurement_str

        print(f"  DAP ID: {dap_id}")
        print(f"  Available data range: {first_measurement_str} to {last_measurement_str}")
        print(f"  Aggregation level: {lowest_agg_level}")

        # Determine date range to fetch
        if DATE_RANGE_MODE == "custom_range" and CUSTOM_START_DATE and CUSTOM_END_DATE:
            start_date = datetime.strptime(CUSTOM_START_DATE, "%Y-%m-%d")
            end_date = datetime.strptime(CUSTOM_END_DATE, "%Y-%m-%d")
            print(f"  Using custom date range: {start_date.date()} to {end_date.date()}")
        else:
            # Use full historical range from metadata
            start_date = datetime.strptime(first_measurement_str, "%Y-%m-%d")
            end_date = datetime.strptime(last_measurement_str, "%Y-%m-%d")
            print(f"  Using full historical range")

        total_days = (end_date - start_date).days + 1
        result['total_days'] = total_days
        print(f"  Total days to fetch: {total_days}")

        # Determine interval based on aggregation level
        if lowest_agg_level == "quarterly":
            interval = "quarterly"
        elif lowest_agg_level == "hourly":
            interval = "hourly"
        else:
            interval = "daily"

        print(f"  Fetching at interval: {interval}")

        # Step 2: Fetch data in chunks
        current_start = start_date
        files_written = 0
        api_calls = 0

        while current_start <= end_date:
            current_end = current_start + timedelta(days=CHUNK_DAYS - 1)
            if current_end > end_date:
                current_end = end_date

            chunk_days = (current_end - current_start).days + 1
            print(f"\n  → Fetching chunk: {current_start.date()} to {current_end.date()} ({chunk_days} days)")

            params = {
                'apikey': API_KEY,
                'dap': dap_id,
                'format': 'json',
                'per': interval,
                'begin': current_start.strftime('%Y-%m-%d'),
                'end': current_end.strftime('%Y-%m-%d')
            }

            try:
                response = requests.get(AGGREGATES_URL, params=params)
                response.raise_for_status()
                chunk_data = response.json()
                api_calls += 1

                # Group records by date (one file per day)
                records_by_date = {}
                for record in chunk_data.get('values', []):
                    if record.get('values'):  # Only process records with actual measurement data
                        record_date = record['datetime'].split('T')[0]
                        if record_date not in records_by_date:
                            records_by_date[record_date] = []
                        records_by_date[record_date].append(record)

                # Write one JSON file per day
                for record_date, day_records in records_by_date.items():
                    # Create combined data structure (same format as active pipeline)
                    combined_data = {
                        "dap_id": dap_id,
                        "ean": ean,  # CRITICAL: Include EAN for bronze/silver processing
                        "date": record_date,
                        "aggregation_level": interval,
                        "records": day_records
                    }

                    # File naming: {dap_id}_{date}_{interval}_{timestamp}.json
                    processing_ts = int(time.time() * 1000)  # milliseconds for uniqueness
                    file_name = f"{dap_id}_{record_date}_{interval}_{processing_ts}.json"
                    output_path = f"{VOLUME_PATH}/{file_name}"

                    # Write to volume
                    json_content = json.dumps(combined_data, indent=4)
                    dbutils.fs.put(output_path, json_content, overwrite=False)
                    files_written += 1

                print(f"     ✓ Wrote {len(records_by_date)} file(s) for this chunk")

            except requests.exceptions.HTTPError as http_err:
                print(f"     ✗ HTTP error: {http_err}")
            except Exception as err:
                print(f"     ✗ Error: {err}")

            # Move to next chunk
            current_start += timedelta(days=CHUNK_DAYS)

        result['files_written'] = files_written
        result['api_calls'] = api_calls
        result['status'] = 'success'
        total_files_written += files_written
        total_api_calls += api_calls
        total_days_covered += total_days

        print(f"\n✓ SUCCESS: {files_written} files written, {api_calls} API calls made")

    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP {e.response.status_code if e.response else 'Error'}"
        print(f"\n✗ FAILED: {error_msg}")
        result['status'] = 'error'
        result['error'] = error_msg

    except Exception as e:
        error_msg = str(e)
        print(f"\n✗ FAILED: {error_msg}")
        result['status'] = 'error'
        result['error'] = error_msg

    results.append(result)

# COMMAND ----------

# DBTITLE 1,Summary Report
print("\n" + "=" * 80)
print("BACKFILL COMPLETE - SUMMARY REPORT")
print("=" * 80)

successful = sum(1 for r in results if r['status'] == 'success')
failed = sum(1 for r in results if r['status'] == 'error')

print(f"\nOverall Statistics:")
print(f"  Total EANs processed: {len(results)}")
print(f"  ✓ Successful: {successful}")
print(f"  ✗ Failed: {failed}")
print(f"  Total files written: {total_files_written:,}")
print(f"  Total API calls: {total_api_calls:,}")
print(f"  Total days covered: {total_days_covered:,}")

print("\n" + "-" * 80)
print("DETAILED RESULTS BY EAN")
print("-" * 80)

for r in results:
    status_icon = "✓" if r['status'] == 'success' else "✗"
    print(f"\n{status_icon} EAN: {r['ean']}")
    print(f"   Location: {r['location']}, {r['city']}")
    print(f"   Entity: {r['entity']}")

    if r['status'] == 'success':
        print(f"   DAP ID: {r['dap_id']}")
        print(f"   Date Range: {r['first_measurement']} to {r['last_measurement']}")
        print(f"   Total Days: {r['total_days']:,}")
        print(f"   Aggregation Level: {r['aggregation_level']}")
        print(f"   Files Written: {r['files_written']:,}")
        print(f"   API Calls: {r['api_calls']}")
    else:
        print(f"   Error: {r['error']}")

print("\n" + "=" * 80)
print("NEXT STEPS")
print("=" * 80)
print("""
1. Verify files in land volume:
   %fs ls {volume_path}

2. Process land → bronze:
   Run notebook: src/clients/acme/batch/wattflow/b02_wattflow_land2bronze.py

3. Process bronze → silver:
   Run notebook: src/clients/acme/batch/wattflow/b03_wattflow_bronze2silver.py

4. Process silver → gold:
   Run dbt models: dbt run --select f_wattflow_dap_batch

5. The inactive EAN data will now be seamlessly integrated with active EAN data
   in the gold layer for analytics and reporting.
""".format(volume_path=VOLUME_PATH))
print("=" * 80)
