# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import requests
import json
import time
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "land", "Schema Name for Volume")
dbutils.widgets.text("volume_name", "wattflow_dap_batch", "Volume Name")
dbutils.widgets.text("time_range_minutes", "1440", "Time Range in Minutes (e.g., 1440=24h, 60=1h, 15=15min)")
dbutils.widgets.text("days_back", "1", "Days back from now (0=today, 1=yesterday)")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
TIME_RANGE_MINUTES = int(dbutils.widgets.get("time_range_minutes"))
DAYS_BACK = int(dbutils.widgets.get("days_back"))
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

# Get API key from secrets
API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="api_key")

# Configure paths
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# API endpoints
BASE_URL = "https://wattflow.e-dataportal.nl/api/v3"
CONNECTIONS_URL = f"{BASE_URL}/connections/"
AGGREGATES_URL = f"{BASE_URL}/aggregates/"

# COMMAND ----------

# DBTITLE 1,Validate and Parse Time Range Configuration
# Validate minimum is 15 minutes (the most granular interval available)
if TIME_RANGE_MINUTES < 15:
    raise ValueError(f"Time range must be at least 15 minutes. Got: {TIME_RANGE_MINUTES} minutes")

# Convert minutes to hours for easier calculation
hours_to_fetch = TIME_RANGE_MINUTES / 60
days_to_fetch = TIME_RANGE_MINUTES / 1440

print(f"Time range configuration:")
print(f"  - Minutes: {TIME_RANGE_MINUTES}")
print(f"  - Hours: {hours_to_fetch:.2f}")
print(f"  - Days: {days_to_fetch:.2f}")

# Helper to display human-readable time range
def format_time_range(minutes):
    if minutes < 60:
        return f"{minutes} minutes"
    elif minutes < 1440:
        hours = minutes / 60
        return f"{hours:.1f} hours" if hours % 1 else f"{int(hours)} hours"
    else:
        days = minutes / 1440
        return f"{days:.1f} days" if days % 1 else f"{int(days)} days"

print(f"  - Human readable: {format_time_range(TIME_RANGE_MINUTES)}")

# COMMAND ----------

# DBTITLE 1,Define Active EANs to Process
# List of active EAN connections from Excel (Status: Actief)
# Note: Some locations have multiple meters, represented as separate EANs
ACTIVE_EANS = [
    "871689260011945210",  # Argonautenweg 57 BIJ, Rotterdam - B&T
    "871689260012433310",  # Orionstraat 235 BIJ, 's-Gravenhage - B&T Speciale Projecten
    "871689260012411691",  # Wegastraat 67 BIJ, 's-Gravenhage - B&T Speciale Projecten
    "871687120000023324",  # Plantijnweg 32, Culemborg - Concernhuisvesting (3 meters)
    "871687120000061975",  # De Serpeling 120, Lelystad - BES
    "871687910000065475",  # De Steenbok 15, 's-Hertogenbosch - BES
    "871687910000475441",  # De Steenbok 15, 's-Hertogenbosch - BES
    "871687910000475458",  # De Steenbok 15, 's-Hertogenbosch - BES
    "871694831000416077",  # Den Hulst 102, Nieuwleusen - BES (3 meters)
    "871694831000080872",  # Den Hulst 110, Nieuwleusen - BES
    "871685900041068209",  # Elsrijkdreef 199 A, Amsterdam - BES
    "871692150000024054",  # H.J.Nederhorststraat 1, Gouda - BES (9 meters)
    "871694831000211504",  # Jeverweg 16 T/M 18, Groningen - BES (3 meters)
    "871689260013155471",  # Kilkade 39, Dordrecht - BES
    "871689276000060611",  # Kilkade 53, Dordrecht - BES (2 meters)
    "871689290602500320",  # Kilkade 53, Dordrecht - BES
    "871687120000032982",  # Marowijne 34, Apeldoorn - BES
    "871690910008547670",  # Molenstraat 60, Zwammerdam - BES
    "871690910000009350",  # Molenstraat 63, Zwammerdam - BES (2 meters)
    "871687140006918639",  # Molenstraat 63, Zwammerdam - BES
    "871687400009183091",  # Proostwetering 31, Utrecht - BES (2 meters)
    "871689260012252874",  # Von Geusaustraat 195 BIJ, Voorburg - Infra Rail
    "871687400008460148",  # Tasveld 16, Montfoort - Infra Telecom
    "871689276000030706",  # Stadionweg 23, Rotterdam - Services Nederland
    "871685920004378541",  # Dijkmeerlaan 551, Amsterdam - Wonen
    "871689260013028706",  # Euryzakade 401 CVZ, Zwijndrecht - Wonen
    "871685920003789768",  # H.J.E. Wenckebachweg 1692, Amsterdam - Wonen
    "871689260013005899",  # Hartenruststraat 6 BIJ, Rotterdam - Wonen Bouw op Maat
    "871688660012152920",  # Van Embdenstrat 2 TA, Delft - Wonen Bouw op Maat
    "871685920004381039",  # Vreeswijkpad 6, Amsterdam - Wonen Bouw op Maat
    "871687110004007611",  # Akulaan 2, Ede - Wonen
    "871685920003998443",  # Bongerdkade 32, Amsterdam - Wonen
    "871685920004053639",  # Mary van der Sluisstraat 428, Amsterdam - Wonen
    "871685920004053752",  # Zuider IJdijk 76 A, Amsterdam - Wonen
    "871685920004541433",  # G.J. Scheurleerpad 8, Amsterdam - Wonen
    "871685920004481388",  # Transvaalstraat 9, Haarlem - Wonen
]

# Remove duplicates (some locations have multiple subcodes for same EAN)
ACTIVE_EANS = list(set([ean.strip() for ean in ACTIVE_EANS]))

print(f"Total unique EANs to process: {len(ACTIVE_EANS)}")

# COMMAND ----------

# DBTITLE 1,Create Landing Volume (if not exists)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON aggregates from the Wattflow API at various granularities.'
""")

# COMMAND ----------

# DBTITLE 1,Calculate Date Range to Fetch
# Calculate end time (e.g., yesterday at 23:59:59 if days_back=1)
reference_time = datetime.now() - timedelta(days=DAYS_BACK)

# For sub-day ranges, we need to work with datetime, not just date
if hours_to_fetch < 24:
    # Fetch most recent N hours from the reference time
    end_datetime = reference_time
    start_datetime = end_datetime - timedelta(hours=hours_to_fetch)

    # Convert to dates for API (API only accepts date format)
    start_date = start_datetime.date()
    end_date = end_datetime.date()
else:
    # For 24h or more, fetch complete days
    end_date = reference_time.date()
    days_to_fetch = int(hours_to_fetch / 24)
    start_date = end_date - timedelta(days=days_to_fetch - 1)

print(f"Date range to fetch:")
print(f"  - Start Date: {start_date}")
print(f"  - End Date: {end_date}")
print(f"  - Hours requested: {hours_to_fetch}")

# COMMAND ----------

# DBTITLE 1,Helper Function to Determine Aggregation Level
def get_best_aggregation_level(lowest_level, hours_requested):
    """
    Determines the best aggregation level based on:
    1. The DAP's lowest_aggregation_level capability
    2. The time range being requested

    Returns: (interval_name, expected_records_per_day)
    """

    # Hierarchy: quarterly (15min) > hourly > daily
    # quarterly = 96 records/day, hourly = 24 records/day, daily = 1 record/day

    if lowest_level == "quarterly":
        # 15-minute intervals (most granular)
        return ("quarterly", 96)
    elif lowest_level == "hourly":
        # 1-hour intervals
        return ("hourly", 24)
    else:
        # daily or any other = daily intervals
        return ("daily", 1)

# COMMAND ----------

# DBTITLE 1,Process All Active EANs
print(f"\n{'='*80}")
print(f"WATTFLOW MULTI-EAN INGESTION - STARTING")
print(f"{'='*80}")
print(f"Processing {len(ACTIVE_EANS)} EANs for date range: {start_date} to {end_date}")
print(f"Time range: {format_time_range(TIME_RANGE_MINUTES)} ({TIME_RANGE_MINUTES} minutes)")
print(f"{'='*80}\n")

# Track statistics
results = []
total_files_written = 0
total_records_written = 0
total_eans_with_data = 0
total_eans_without_data = 0
total_eans_failed = 0

# Aggregation level statistics
granularity_stats = {"quarterly": 0, "hourly": 0, "daily": 0}

for idx, ean in enumerate(ACTIVE_EANS, 1):
    print(f"\n[{idx}/{len(ACTIVE_EANS)}] Processing EAN: {ean}")
    print("-" * 80)

    result = {
        "ean": ean,
        "status": "pending",
        "dap_id": None,
        "aggregation_level": None,
        "records_written": 0,
        "files_written": 0,
        "error": None
    }

    try:
        # Step 1: Get metadata for this EAN
        print(f"  → Fetching metadata...")
        params = {
            "apikey": API_KEY,
            "ean": ean,
            "format": "json"
        }

        response = requests.get(CONNECTIONS_URL, params=params)
        response.raise_for_status()

        metadata = response.json()['values'][0]
        dap_id = str(metadata['id'])
        first_measurement = metadata['first-measurement']
        last_measurement = metadata['last-measurement']
        lowest_agg_level = metadata.get('lowest_aggregation_level', 'daily')

        result["dap_id"] = dap_id
        result["aggregation_level"] = lowest_agg_level

        print(f"  → DAP ID: {dap_id}")
        print(f"  → Data range: {first_measurement} to {last_measurement}")
        print(f"  → Lowest aggregation level: {lowest_agg_level}")

        # Step 2: Determine best aggregation level
        interval, expected_records_per_day = get_best_aggregation_level(lowest_agg_level, hours_to_fetch)

        print(f"  → Using interval: {interval} (~{expected_records_per_day} records/day)")
        granularity_stats[interval] += 1

        # Step 3: Fetch aggregates at the determined interval
        print(f"  → Fetching aggregates...")
        params = {
            'apikey': API_KEY,
            'dap': dap_id,
            'format': 'json',
            'per': interval,
            'begin': start_date.strftime('%Y-%m-%d'),
            'end': end_date.strftime('%Y-%m-%d')
        }

        response = requests.get(AGGREGATES_URL, params=params)
        response.raise_for_status()

        data = response.json()
        all_records = data.get('values', [])

        # Step 4: Check if we have data with non-empty values
        records_with_data = [r for r in all_records if r.get('values', [])]

        if not records_with_data:
            print(f"  ⚠ No data available (empty values array)")
            result["status"] = "no_data"
            total_eans_without_data += 1
            continue

        print(f"  → Received {len(records_with_data)} records with data")

        # Step 5: Group records by date and write one file per day
        # This handles quarterly/hourly data by grouping all intervals for same day
        records_by_date = {}
        for record in records_with_data:
            record_date = record['datetime'].split('T')[0]
            if record_date not in records_by_date:
                records_by_date[record_date] = []
            records_by_date[record_date].append(record)

        # Step 6: Write files (one per date, containing all intervals for that date)
        files_written = 0
        total_records = 0

        for record_date, day_records in records_by_date.items():
            try:
                # Create a combined structure for all intervals in this day
                combined_data = {
                    "dap_id": dap_id,
                    "ean": ean,  # Add EAN to flow through bronze → silver
                    "date": record_date,
                    "aggregation_level": interval,
                    "records": day_records
                }

                processing_ts = int(time.time() * 1000)  # milliseconds for uniqueness
                file_name = f"{dap_id}_{record_date}_{interval}_{processing_ts}.json"
                output_path = f"{VOLUME_PATH}/{file_name}"

                json_content = json.dumps(combined_data, indent=4)
                dbutils.fs.put(output_path, json_content, overwrite=False)
                files_written += 1
                total_records += len(day_records)

            except Exception as e:
                print(f"  ✗ Failed to write file for {record_date}: {e}")

        result["files_written"] = files_written
        result["records_written"] = total_records
        result["status"] = "success"
        total_files_written += files_written
        total_records_written += total_records
        total_eans_with_data += 1

        print(f"  ✓ Success: {files_written} file(s), {total_records} total records")

    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP {e.response.status_code if e.response else 'Error'}"
        print(f"  ✗ Failed: {error_msg}")
        result["status"] = "error"
        result["error"] = error_msg
        total_eans_failed += 1

    except Exception as e:
        error_msg = str(e)
        print(f"  ✗ Failed: {error_msg}")
        result["status"] = "error"
        result["error"] = error_msg
        total_eans_failed += 1

    results.append(result)
