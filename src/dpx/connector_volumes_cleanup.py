# Databricks notebook source
# DBTITLE 1,Imports and Configuration
"""
Auto-discovery connector-based volume cleanup job.

This notebook:
1. Auto-discovers all volumes in each catalog's land layer that match connector name patterns
2. Creates/updates a configuration table with discovered volumes
3. Performs TTL cleanup operations on all discovered volumes

Parameters:
    - connector_names: Comma-separated list of connector names to process (e.g., 'voltcore,sunpeak,solarflow')
    - retention_days: Default retention period in days (default: 15)
    - catalogs_to_scan: Comma-separated list of catalog patterns to scan (default: '*_dev,*_prod')
"""
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Widget Configuration
dbutils.widgets.text("connector_names", "voltcore,sunpeak,solarflow,wattflow", "Connector Names (comma-separated)")
dbutils.widgets.text("retention_days", "15", "Default Retention Days")
dbutils.widgets.text("catalogs_to_scan", "*_dev,*_prod", "Catalog Patterns (comma-separated)")

CONNECTOR_NAMES = [name.strip() for name in dbutils.widgets.get("connector_names").split(",")]
DEFAULT_RETENTION_DAYS = int(dbutils.widgets.get("retention_days"))
CATALOG_PATTERNS = [pattern.strip() for pattern in dbutils.widgets.get("catalogs_to_scan").split(",")]

# Configuration table location - stored in dpx catalog
CONFIG_CATALOG = "dpx_dev"
CONFIG_SCHEMA = "operational"
CONFIG_TABLE = "connector_volume_config"
CONFIG_TABLE_PATH = f"{CONFIG_CATALOG}.{CONFIG_SCHEMA}.{CONFIG_TABLE}"

print(f"{'='*60}")
print(f"AUTO-DISCOVERY CONNECTOR-BASED VOLUME CLEANUP")
print(f"{'='*60}")
print(f"Connectors to process: {', '.join(CONNECTOR_NAMES)}")
print(f"Default retention: {DEFAULT_RETENTION_DAYS} days")
print(f"Catalog patterns: {', '.join(CATALOG_PATTERNS)}")
print(f"Config table: {CONFIG_TABLE_PATH}")
print(f"{'='*60}\n")

# COMMAND ----------

# DBTITLE 1,Create Configuration Table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CONFIG_TABLE_PATH} (
        connector_name STRING COMMENT 'API connector name (e.g., voltcore, sunpeak, solarflow)',
        client_catalog STRING COMMENT 'Client catalog name (e.g., globex_dev, acme_dev)',
        volume_name STRING COMMENT 'Volume name in the land schema',
        retention_days INT COMMENT 'Number of days to retain files before deletion',
        last_discovered TIMESTAMP COMMENT 'When this volume was last discovered',
        last_cleaned TIMESTAMP COMMENT 'When this volume was last cleaned',
        files_deleted_last_run INT COMMENT 'Files deleted in last cleanup run',
        bytes_deleted_last_run BIGINT COMMENT 'Bytes deleted in last cleanup run'
    )
    COMMENT 'Auto-discovered configuration for connector-based volume cleanup operations'
""")

print(f"✅ Configuration table ready: {CONFIG_TABLE_PATH}\n")

# COMMAND ----------

# DBTITLE 1,Auto-Discover Volumes
print("🔍 Auto-discovering volumes...\n")

# Get all catalogs
all_catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]

# Filter catalogs based on patterns
import fnmatch
catalogs_to_process = []
for catalog in all_catalogs:
    for pattern in CATALOG_PATTERNS:
        if fnmatch.fnmatch(catalog, pattern):
            catalogs_to_process.append(catalog)
            break

print(f"Found {len(catalogs_to_process)} catalog(s) matching patterns: {', '.join(catalogs_to_process)}\n")

# Discover volumes in each catalog's land layer
discovered_volumes = []

for catalog in catalogs_to_process:
    try:
        # Check if catalog has a 'land' schema
        schemas = [row.databaseName for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]

        if 'land' not in schemas:
            print(f"  ⚠️  {catalog}: No 'land' schema found, skipping")
            continue

        # Use Unity Catalog metadata to list volumes in the land schema
        try:
            volumes_df = spark.sql(f"SHOW VOLUMES IN {catalog}.land")
            volumes = [row.volume_name for row in volumes_df.collect()]

            if not volumes:
                print(f"  ℹ️  {catalog}: No volumes in land schema")
                continue

            # Check each volume against connector names
            for volume_name in volumes:
                # Check if volume name starts with any connector name
                for connector_name in CONNECTOR_NAMES:
                    if volume_name.startswith(connector_name):
                        discovered_volumes.append({
                            'connector_name': connector_name,
                            'client_catalog': catalog,
                            'volume_name': volume_name,
                            'retention_days': DEFAULT_RETENTION_DAYS
                        })
                        print(f"  ✅ {catalog}/land/{volume_name} → {connector_name} connector")
                        break

        except Exception as e:
            print(f"  ⚠️  {catalog}: Cannot list volumes in land schema - {e}")
            continue

    except Exception as e:
        print(f"  ⚠️  {catalog}: Error processing catalog - {e}")
        continue

print(f"\n📊 Discovered {len(discovered_volumes)} volume(s) across {len(set(v['connector_name'] for v in discovered_volumes))} connector(s)\n")

# Display discovered volumes grouped by connector
if discovered_volumes:
    print("Discovered volumes by connector:")
    for connector in CONNECTOR_NAMES:
        connector_vols = [v for v in discovered_volumes if v['connector_name'] == connector]
        if connector_vols:
            print(f"\n  {connector.upper()} ({len(connector_vols)} volume(s)):")
            for vol in connector_vols:
                print(f"    • {vol['client_catalog']}/land/{vol['volume_name']}")
    print()

# COMMAND ----------

# DBTITLE 1,Update Configuration Table
if discovered_volumes:
    # Create DataFrame from discovered volumes
    from pyspark.sql.functions import current_timestamp, lit

    discovered_df = spark.createDataFrame(discovered_volumes)
    discovered_df = discovered_df.withColumn("last_discovered", current_timestamp())

    discovered_df.createOrReplaceTempView("discovered_volumes_temp")

    # Merge into configuration table
    spark.sql(f"""
        MERGE INTO {CONFIG_TABLE_PATH} AS target
        USING discovered_volumes_temp AS source
        ON target.connector_name = source.connector_name
            AND target.client_catalog = source.client_catalog
            AND target.volume_name = source.volume_name
        WHEN MATCHED THEN
            UPDATE SET
                retention_days = source.retention_days,
                last_discovered = source.last_discovered
        WHEN NOT MATCHED THEN
            INSERT (connector_name, client_catalog, volume_name, retention_days, last_discovered, last_cleaned, files_deleted_last_run, bytes_deleted_last_run)
            VALUES (source.connector_name, source.client_catalog, source.volume_name, source.retention_days, source.last_discovered, NULL, 0, 0)
    """)

    print(f"✅ Configuration table updated with {len(discovered_volumes)} volume(s)\n")
else:
    print("⚠️  No volumes discovered. Exiting without cleanup.\n")
    dbutils.notebook.exit("SKIPPED: No volumes discovered matching connector patterns")

# COMMAND ----------

# DBTITLE 1,Display Current Configuration
print("Current configuration table:")
display(spark.sql(f"""
    SELECT
        connector_name,
        client_catalog,
        volume_name,
        retention_days,
        last_discovered,
        last_cleaned,
        files_deleted_last_run,
        ROUND(bytes_deleted_last_run / POWER(1024, 3), 2) as gb_deleted_last_run
    FROM {CONFIG_TABLE_PATH}
    ORDER BY connector_name, client_catalog, volume_name
"""))

# COMMAND ----------

# DBTITLE 1,Cleanup Function
def cleanup_volume_files(volume_path, retention_days, catalog_name, volume_name):
    """
    Remove all files older than retention_days from volume.
    Uses file modification time to determine file age.
    Handles both flat structures and nested directory structures.

    Args:
        volume_path: Path to the volume to clean
        retention_days: Number of days to retain files
        catalog_name: Catalog name for logging purposes
        volume_name: Volume name for logging purposes

    Returns:
        Tuple of (files_deleted, bytes_deleted, oldest_file_path, file_types_deleted)
    """
    cutoff_timestamp = (datetime.now() - timedelta(days=retention_days)).timestamp() * 1000  # Convert to milliseconds
    cutoff_date = datetime.now() - timedelta(days=retention_days)

    deleted_files = []      # List of deleted file paths
    total_bytes = 0         # Total bytes freed
    oldest_file = None      # Track oldest file deleted
    oldest_timestamp = None # Track timestamp of oldest file
    file_types = set()      # Track file extensions deleted

    print(f"{'='*60}")
    print(f"CLEANUP: {catalog_name}/land/{volume_name}")
    print(f"{'='*60}")
    print(f"Retention: {retention_days} days")
    print(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    try:
        # Check if volume exists
        try:
            items_in_volume = dbutils.fs.ls(volume_path)
        except Exception as e:
            print(f"⚠️  Volume does not exist or is not accessible")
            print(f"   Skipping...\n")
            return 0, 0, None, 'none'

        def process_directory_recursive(dir_path, indent=""):
            """Recursively process directories and delete old files"""
            nonlocal deleted_files, total_bytes, oldest_file, oldest_timestamp, file_types

            try:
                items = dbutils.fs.ls(dir_path)
            except Exception as e:
                print(f"{indent}⚠️  Cannot access: {dir_path}")
                return

            # Separate files and directories
            files = [item for item in items if not item.isDir()]
            dirs = [item for item in items if item.isDir()]

            # Process files in current directory
            files_to_delete = []
            for file in files:
                if file.modificationTime < cutoff_timestamp:
                    files_to_delete.append(file)

            if files_to_delete:
                print(f"{indent}Found {len(files_to_delete)} old file(s) to delete")

                for file in files_to_delete:
                    file_date = datetime.fromtimestamp(file.modificationTime / 1000)
                    age_days = (datetime.now() - file_date).days

                    # Track file extension
                    file_ext = file.name.split('.')[-1] if '.' in file.name else 'no_extension'
                    file_types.add(file_ext)

                    # Show first few files being deleted
                    if len(deleted_files) < 3:
                        size_mb = file.size / (1024 * 1024)
                        print(f"{indent}  • {file.name} ({size_mb:.2f} MB, {age_days} days old)")
                    elif len(deleted_files) == 3:
                        print(f"{indent}  • ... and more files ...")

                    # Delete the file
                    try:
                        dbutils.fs.rm(file.path)
                        deleted_files.append(file.path)
                        total_bytes += file.size

                        # Track oldest file
                        if not oldest_timestamp or file.modificationTime < oldest_timestamp:
                            oldest_timestamp = file.modificationTime
                            oldest_file = file.path
                    except Exception as e:
                        print(f"{indent}⚠️  Failed to delete {file.name}: {e}")

            # Recursively process subdirectories
            for dir_item in dirs:
                process_directory_recursive(dir_item.path, indent + "  ")

        # Start recursive processing from the root volume path
        process_directory_recursive(volume_path)

    except Exception as e:
        print(f"\n❌ Error during cleanup: {e}")
        raise

    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY: {catalog_name}/land/{volume_name}")
    print(f"{'='*60}")
    print(f"Files deleted: {len(deleted_files):,}")
    print(f"Space freed: {total_bytes / (1024**3):.2f} GB")
    print(f"File types: {', '.join(sorted(file_types)) if file_types else 'none'}")
    print(f"{'='*60}\n")

    return len(deleted_files), total_bytes, oldest_file, ','.join(sorted(file_types)) if file_types else 'none'

# COMMAND ----------

# DBTITLE 1,Execute Cleanup for All Discovered Volumes
# Load current configuration
configs_df = spark.sql(f"""
    SELECT
        connector_name,
        client_catalog,
        volume_name,
        retention_days
    FROM {CONFIG_TABLE_PATH}
    ORDER BY connector_name, client_catalog, volume_name
""")

configs = configs_df.collect()

# Track aggregate statistics
total_files_deleted = 0
total_bytes_deleted = 0
cleanup_results = []

print(f"{'#'*60}")
print(f"STARTING CLEANUP FOR {len(configs)} VOLUME(S)")
print(f"{'#'*60}\n")

for config in configs:
    connector_name = config.connector_name
    catalog_name = config.client_catalog
    volume_name = config.volume_name
    retention_days = config.retention_days
    volume_path = f"/Volumes/{catalog_name}/land/{volume_name}"

    try:
        files_deleted, bytes_deleted, oldest_file, file_types_deleted = cleanup_volume_files(
            volume_path,
            retention_days,
            catalog_name,
            volume_name
        )

        # Store results
        cleanup_results.append({
            'connector_name': connector_name,
            'catalog_name': catalog_name,
            'volume_name': volume_name,
            'volume_path': volume_path,
            'retention_days': retention_days,
            'files_deleted': files_deleted,
            'bytes_deleted': bytes_deleted,
            'oldest_file': oldest_file,
            'file_types_deleted': file_types_deleted,
            'status': 'SUCCESS'
        })

        total_files_deleted += files_deleted
        total_bytes_deleted += bytes_deleted

    except Exception as e:
        print(f"❌ Failed to clean {catalog_name}/{volume_name}: {e}\n")
        cleanup_results.append({
            'connector_name': connector_name,
            'catalog_name': catalog_name,
            'volume_name': volume_name,
            'volume_path': volume_path,
            'retention_days': retention_days,
            'files_deleted': 0,
            'bytes_deleted': 0,
            'oldest_file': None,
            'file_types_deleted': 'none',
            'status': f'FAILED: {str(e)}'
        })

# COMMAND ----------

# DBTITLE 1,Update Configuration Table with Cleanup Results
from pyspark.sql.functions import current_timestamp

for result in cleanup_results:
    if result['status'] == 'SUCCESS':
        spark.sql(f"""
            UPDATE {CONFIG_TABLE_PATH}
            SET last_cleaned = current_timestamp(),
                files_deleted_last_run = {result['files_deleted']},
                bytes_deleted_last_run = {result['bytes_deleted']}
            WHERE connector_name = '{result['connector_name']}'
              AND client_catalog = '{result['catalog_name']}'
              AND volume_name = '{result['volume_name']}'
        """)

print("✅ Configuration table updated with cleanup results\n")

# COMMAND ----------

# DBTITLE 1,Log Cleanup Activity to Client Audit Tables
# Log to each client's operational schema
for result in cleanup_results:
    if result['status'] == 'SUCCESS' and result['files_deleted'] > 0:
        catalog_name = result['catalog_name']
        cleanup_table_path = f"{catalog_name}.operational.volume_cleanup_log"

        try:
            # Ensure the cleanup log table exists in the client catalog
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cleanup_table_path} (
                    cleanup_timestamp TIMESTAMP COMMENT 'When the cleanup job ran',
                    catalog_name STRING COMMENT 'Unity Catalog name',
                    volume_path STRING COMMENT 'Full path to the volume including volume name',
                    retention_days INT COMMENT 'Number of days files were retained',
                    data_type STRING COMMENT 'Pattern of files deleted (e.g., json, csv, parquet)',
                    files_deleted INT COMMENT 'Count of files removed',
                    bytes_deleted BIGINT COMMENT 'Total bytes freed up',
                    oldest_file_deleted STRING COMMENT 'Path of the oldest file that was deleted',
                    connector_name STRING COMMENT 'API connector name that triggered this cleanup'
                )
                COMMENT 'Audit log for volume TTL cleanup operations'
            """)

            # Insert cleanup record
            oldest_file_sql = f"'{result['oldest_file']}'" if result['oldest_file'] else 'NULL'
            spark.sql(f"""
                INSERT INTO {cleanup_table_path}
                VALUES (
                    current_timestamp(),
                    '{result['catalog_name']}',
                    '{result['volume_path']}',
                    {result['retention_days']},
                    '{result['file_types_deleted']}',
                    {result['files_deleted']},
                    {result['bytes_deleted']},
                    {oldest_file_sql},
                    '{result['connector_name']}'
                )
            """)

            print(f"✅ Logged cleanup for {catalog_name} to: {cleanup_table_path}")

        except Exception as e:
            print(f"⚠️  Failed to log cleanup for {catalog_name}: {e}")

# COMMAND ----------

# DBTITLE 1,Overall Summary
print(f"\n{'='*60}")
print(f"OVERALL CLEANUP SUMMARY")
print(f"{'='*60}")
print(f"Connectors: {', '.join(CONNECTOR_NAMES)}")
print(f"Volumes processed: {len(cleanup_results)}")
print(f"Total files deleted: {total_files_deleted:,}")
print(f"Total space freed: {total_bytes_deleted / (1024**3):.2f} GB")
print(f"{'='*60}\n")

# Show results grouped by connector
print("Results by connector:\n")
for connector in CONNECTOR_NAMES:
    connector_results = [r for r in cleanup_results if r['connector_name'] == connector]
    if connector_results:
        connector_files = sum(r['files_deleted'] for r in connector_results)
        connector_bytes = sum(r['bytes_deleted'] for r in connector_results)
        print(f"  {connector.upper()}:")
        print(f"    Volumes: {len(connector_results)}")
        print(f"    Files deleted: {connector_files:,}")
        print(f"    Space freed: {connector_bytes / (1024**3):.2f} GB")
        for result in connector_results:
            status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
            print(f"      {status_icon} {result['catalog_name']}/{result['volume_name']}: {result['files_deleted']} files")
        print()

# COMMAND ----------

# DBTITLE 1,Return Status for Job Monitoring
success_count = sum(1 for r in cleanup_results if r['status'] == 'SUCCESS')
if success_count == len(cleanup_results):
    dbutils.notebook.exit(f"✅ SUCCESS: Deleted {total_files_deleted} files ({total_bytes_deleted / (1024**3):.2f} GB) across {len(cleanup_results)} volume(s)")
else:
    dbutils.notebook.exit(f"⚠️  PARTIAL SUCCESS: {success_count}/{len(cleanup_results)} volumes processed. Deleted {total_files_deleted} files ({total_bytes_deleted / (1024**3):.2f} GB)")
