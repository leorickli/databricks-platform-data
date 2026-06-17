# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import os
import json
import requests
from datetime import datetime, timedelta
import time

API_KEY = dbutils.secrets.get(scope="globex_sunpeak_api_creds", key="api_key")

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "sunpeak_inverters_batch", "Volume name for the API files")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

dbutils.widgets.text("backfill_months", "12", "Number of months to backfill (default: 12 for 1 year)")
BACKFILL_MONTHS = int(dbutils.widgets.get("backfill_months"))

dbutils.widgets.text("site_id_filter", "", "Optional: Specific site ID to backfill (leave empty for all sites)")
SITE_ID_FILTER = dbutils.widgets.get("site_id_filter")

BASE_URL = "https://monitoringapi.sunpeak.com"
SITES_LIST_URL = f"{BASE_URL}/sites/list"

# API rate limiting settings
REQUESTS_PER_DAY = 300
MAX_CONCURRENT_REQUESTS = 3
REQUEST_DELAY_SECONDS = 1  # Delay between requests to avoid rate limiting

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON files from Sunpeak API'
""")

# COMMAND ----------

# DBTITLE 1,Helper Function for Date Range Generation
def generate_weekly_ranges(start_date, end_date):
    """
    Generate list of (start, end) date tuples for 1-week intervals.
    Used for Inverter Technical Data endpoint (1-week max).
    """
    ranges = []
    current = start_date

    while current < end_date:
        # Calculate end of current week or end_date, whichever is earlier
        next_week = current + timedelta(days=7)
        range_end = min(next_week, end_date)

        ranges.append((current, range_end))
        current = range_end

    return ranges

# COMMAND ----------

# DBTITLE 1,Fetch All Site IDs
params = {"api_key": API_KEY}

sites_list_response = requests.get(SITES_LIST_URL, params=params)
sites_list_response.raise_for_status()

sites_data = sites_list_response.json()["sites"]
all_site_ids = [site["id"] for site in sites_data["site"]]
site_names = {site["id"]: site["name"] for site in sites_data["site"]}

# Filter by specific site if provided
if SITE_ID_FILTER:
    site_ids = [int(SITE_ID_FILTER)]
    print(f"Backfilling specific site: {SITE_ID_FILTER}")
else:
    site_ids = all_site_ids
    print(f"Backfilling all {len(site_ids)} site(s)")

print(f"\nSites to backfill:")
for site_id in site_ids:
    print(f"  - Site ID {site_id}: {site_names.get(site_id, 'Unknown')}")

# COMMAND ----------

# DBTITLE 1,Calculate Backfill Date Range
# End date: today
end_date = datetime.now()

# Start date: N months ago
start_date = end_date - timedelta(days=BACKFILL_MONTHS * 30)  # Approximate

print(f"\nBackfill period:")
print(f"  Start: {start_date.strftime('%Y-%m-%d')}")
print(f"  End: {end_date.strftime('%Y-%m-%d')}")
print(f"  Duration: ~{BACKFILL_MONTHS} months")

# COMMAND ----------

# DBTITLE 1,Backfill Inverter Technical Data (1-week chunks)
print("\n" + "="*80)
print("BACKFILLING INVERTER TECHNICAL DATA (1-week chunks)")
print("="*80)

inverter_files_saved = 0
inverter_requests_made = 0

for site_id in site_ids:
    print(f"\n--- Site {site_id}: {site_names.get(site_id, 'Unknown')} ---")

    # First, get the list of inverters for this site
    try:
        equipment_url = f"{BASE_URL}/equipment/{site_id}/list"
        equipment_params = {"api_key": API_KEY}

        equipment_response = requests.get(equipment_url, params=equipment_params)
        equipment_response.raise_for_status()
        equipment_data = equipment_response.json()

        inverters = equipment_data.get("reporters", {}).get("list", [])

        if not inverters:
            print(f"  ⚠ No inverters found for site {site_id}")
            continue

        print(f"  Found {len(inverters)} inverter(s)")

        # Generate weekly date ranges (1-week max for inverter technical data)
        weekly_ranges = generate_weekly_ranges(start_date, end_date)
        print(f"  Fetching {len(weekly_ranges)} weekly chunks per inverter")

        # Fetch data for each inverter
        for inverter in inverters:
            serial_number = inverter.get("serialNumber")
            inverter_name = inverter.get("name", "Unknown")

            if not serial_number:
                print(f"    ⚠ Skipping inverter without serial number: {inverter_name}")
                continue

            print(f"\n    Inverter: {inverter_name} ({serial_number})")

            for i, (range_start, range_end) in enumerate(weekly_ranges, 1):
                try:
                    # Format dates for API
                    start_str = range_start.strftime("%Y-%m-%d %H:%M:%S")
                    end_str = range_end.strftime("%Y-%m-%d %H:%M:%S")

                    if i % 10 == 0 or i == 1 or i == len(weekly_ranges):
                        print(f"      [{i}/{len(weekly_ranges)}] {range_start.strftime('%Y-%m-%d')} to {range_end.strftime('%Y-%m-%d')}")

                    # API request
                    inverter_data_url = f"{BASE_URL}/equipment/{site_id}/{serial_number}/data"
                    params = {
                        "api_key": API_KEY,
                        "startTime": start_str,
                        "endTime": end_str
                    }

                    inverter_response = requests.get(inverter_data_url, params=params)
                    inverter_response.raise_for_status()
                    inverter_data = inverter_response.json()
                    inverter_requests_made += 1

                    # Check if there's actual data
                    telemetries = inverter_data.get("data", {}).get("telemetries", [])
                    if not telemetries:
                        continue

                    # Save to volume with serial number in filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"sunpeak_inverter_{site_id}_{serial_number}_{timestamp}_backfill_{range_start.strftime('%Y%m%d')}.json"
                    file_path = os.path.join(VOLUME_PATH, filename)

                    with open(file_path, 'w') as f:
                        json.dump(inverter_data, f, indent=2)

                    inverter_files_saved += 1

                    if i % 10 == 0 or i == 1 or i == len(weekly_ranges):
                        print(f"          ✓ Saved {len(telemetries)} telemetries")

                    # Rate limiting: sleep between requests
                    time.sleep(REQUEST_DELAY_SECONDS)

                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 429:
                        print(f"          Rate limit exceeded - waiting 60 seconds...")
                        time.sleep(60)
                    # Skip logging every HTTP error to reduce noise
                    continue
                except Exception as e:
                    continue

            print(f"      ✓ Completed backfill for {inverter_name}")

    except Exception as e:
        print(f"  ✗ Error fetching equipment list for site {site_id}: {e}")
        continue

print(f"\n✓ Inverter backfill complete: {inverter_files_saved} files saved, {inverter_requests_made} API requests")

# COMMAND ----------

# DBTITLE 1,Backfill Summary
print("\n" + "="*80)
print("BACKFILL JOB COMPLETE")
print("="*80)

print(f"\nFiles saved to volume:")
print(f"  Inverter technical data files: {inverter_files_saved}")

print(f"\nAPI requests made:")
print(f"  Inverter data requests: {inverter_requests_made}")

print(f"\nAPI rate limit status:")
print(f"  Daily limit: {REQUESTS_PER_DAY} requests/day")
print(f"  Requests used: {inverter_requests_made}")
print(f"  Remaining: {REQUESTS_PER_DAY - inverter_requests_made}")

if inverter_requests_made >= REQUESTS_PER_DAY:
    print(f"\n⚠ WARNING: Rate limit may have been exceeded!")
    print(f"  Consider running backfill across multiple days or reducing backfill_months parameter")

print(f"\nData saved to: {VOLUME_PATH}")
print(f"\nNext steps:")
print(f"  1. Run b02_sunpeak_land2bronze.py to process JSON files into bronze tables")
print(f"  2. Run b03_sunpeak_bronze2silver.py to create enriched silver tables")
print(f"  3. Run dbt models to calculate interval energy in the gold layer")

print(f"\nNote:")
print(f"  - Power and energy data are now derived from inverter technical data")
print(f"  - Interval energy is calculated in the gold layer using LAG window functions")
print(f"  - This approach provides per-inverter metrics and supports multiple inverters per site")

# Set task values for downstream processing
dbutils.jobs.taskValues.set(key="total_files_saved", value=inverter_files_saved)
dbutils.jobs.taskValues.set(key="total_api_requests", value=inverter_requests_made)
dbutils.jobs.taskValues.set(key="backfill_start_date", value=start_date.strftime("%Y-%m-%d"))
dbutils.jobs.taskValues.set(key="backfill_end_date", value=end_date.strftime("%Y-%m-%d"))
