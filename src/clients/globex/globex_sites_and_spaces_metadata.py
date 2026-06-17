# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType

# Widget for catalog name
dbutils.widgets.text("catalog_name", "globex_dev", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

# Get credentials from Databricks secrets
VOLTCORE_USERNAME = dbutils.secrets.get(scope="globex_voltcore_api_credentials", key="username")
VOLTCORE_PASSWORD = dbutils.secrets.get(scope="globex_voltcore_api_credentials", key="password")
SUNPEAK_API_KEY = dbutils.secrets.get(scope="globex_sunpeak_api_creds", key="api_key")
SOLARFLOW_API_KEY = dbutils.secrets.get(scope="globex_solarflow_api_creds", key="api_key")

# API Endpoints
VOLTCORE_LOGIN_URL = "https://vrmapi.voltcoreenergy.com/v2/auth/login"
VOLTCORE_INSTALLATIONS_URL = "https://vrmapi.voltcoreenergy.com/v2/users/{idUser}/installations"
SUNPEAK_SITES_URL = "https://monitoringapi.sunpeak.com/sites/list"
SOLARFLOW_PLANTS_URL = "https://openapi.solarflow.com/v1/plant/list"

# COMMAND ----------

# DBTITLE 1,Create Gold Schema if Not Exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.gold COMMENT 'Gold layer - business-ready dimensional tables'")

# COMMAND ----------

# DBTITLE 1,Create dim_site Table Based on Haystack Ontology
dim_site_table = f"{CATALOG_NAME}.gold.dim_site"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {dim_site_table} (
    -- Surrogate Key (Primary Key)
    sk_site STRING COMMENT 'Surrogate key - MD5 hash of (connector + source_site_id)',

    -- Natural Key (Haystack ontology: id → ERD: id_site)
    id_site STRING COMMENT 'Natural key - site identifier with connector prefix (e.g., voltcore-407966) (Haystack: id, materializeAs: id_site)',

    -- Required fields per Haystack ontology
    primary_function STRING COMMENT 'Primary function of building (e.g., residential, commercial, industrial) (Haystack: primaryFunction)',

    -- Display and identification
    dis STRING COMMENT 'Display name for the site (Haystack: dis)',

    -- Geographic address fields (Haystack: geoPlace tags)
    geo_addr STRING COMMENT 'Free form street address (Haystack: geoAddr)',
    geo_street STRING COMMENT 'Geographic street address (Haystack: geoStreet)',
    geo_city STRING COMMENT 'Geographic city or locality name (Haystack: geoCity)',
    geo_coord STRING COMMENT 'Geographic coordinate as C(latitude,longitude) (Haystack: geoCoord)',
    geo_country STRING COMMENT 'Geographic country as ISO 3166-1 two letter code (Haystack: geoCountry)',
    geo_county STRING COMMENT 'Geographic subdivision of state (Haystack: geoCounty)',
    geo_postal_code STRING COMMENT 'Geographic postal code (Haystack: geoPostalCode)',
    geo_state STRING COMMENT 'State or province name (Haystack: geoState)',

    -- Site-specific attributes
    area DECIMAL(10,2) COMMENT 'Area of site in m² (Haystack: area)',
    tz STRING COMMENT 'Timezone identifier from standard timezone database (Haystack: tz)',
    year_built INT COMMENT 'Original year of construction (Haystack: yearBuilt)',

    -- Metadata
    connector STRING COMMENT 'Data source connector (voltcore_api, sunpeak_api, solarflow_api)',
    source_site_id STRING COMMENT 'Original site ID from source system (e.g., 407966)',
    other STRING COMMENT 'JSON string for additional connector-specific fields',
    metadata_updated_at TIMESTAMP COMMENT 'Last metadata update timestamp'
)
CLUSTER BY (connector, sk_site)
COMMENT 'dim_site: Haystack-compliant site dimension table for all GLOBEX sites (Voltcore, Sunpeak, Solarflow). Uses surrogate key sk_site as PK.'
""")

print(f"✓ Created/verified table: {dim_site_table}")

# COMMAND ----------

# DBTITLE 1,Create dim_space Table Based on Haystack Ontology
dim_space_table = f"{CATALOG_NAME}.gold.dim_space"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {dim_space_table} (
    -- Surrogate Key (Primary Key)
    sk_space STRING COMMENT 'Surrogate key - MD5 hash of (sk_site + space_name)',

    -- Natural Key (Haystack ontology: id → ERD: id_space)
    id_space STRING COMMENT 'Natural key - space identifier (e.g., voltcore-407966-space-default) (Haystack: id, materializeAs: id_space)',

    -- Foreign Key (Haystack ontology: siteRef → ERD: sk_site)
    sk_site STRING COMMENT 'Foreign key to dim_site.sk_site (Haystack: siteRef, materializeAs: sk_site)',
    id_site STRING COMMENT 'Natural key reference to parent site (Haystack: siteRef, materializeAs: id_site)',

    -- Display
    dis STRING COMMENT 'Display name for the space (Haystack: dis)',

    -- Space-specific attributes
    area DECIMAL(10,2) COMMENT 'Area of floor space in m² (Haystack: area)',

    -- Metadata
    connector STRING COMMENT 'Data source connector (voltcore_api, sunpeak_api, solarflow_api)',
    other STRING COMMENT 'JSON string for additional connector-specific fields',
    metadata_updated_at TIMESTAMP COMMENT 'Last metadata update timestamp'
)
CLUSTER BY (connector, sk_space)
COMMENT 'dim_space: Haystack-compliant space dimension table - each site has one default space to contain equipment. Uses surrogate key sk_space as PK.'
""")

print(f"✓ Created/verified table: {dim_space_table}")

# COMMAND ----------

# DBTITLE 1,Fetch Voltcore Sites
print("="*60)
print("FETCHING VOLTCORE SITES")
print("="*60)

voltcore_sites = []

try:
    # Login to Voltcore API
    login_payload = {
        "username": VOLTCORE_USERNAME,
        "password": VOLTCORE_PASSWORD,
        "remember_me": False
    }

    login_response = requests.post(VOLTCORE_LOGIN_URL, json=login_payload)
    login_response.raise_for_status()

    token_data = login_response.json()
    bearer_token = token_data["token"]
    user_id = token_data["idUser"]

    headers = {"x-authorization": f"Bearer {bearer_token}"}

    print(f"✓ Authenticated as user ID: {user_id}")

    # Fetch installations
    installations_url = VOLTCORE_INSTALLATIONS_URL.format(idUser=user_id)
    installations_response = requests.get(installations_url, headers=headers)
    installations_response.raise_for_status()

    installations_data = installations_response.json()

    for record in installations_data["records"]:
        site_id = f"voltcore-{record['idSite']}"

        # Parse coordinates
        geo_coord = None
        # Voltcore doesn't provide coordinates in installations endpoint

        # Store additional fields
        other_fields = {
            "access_level": record.get("accessLevel"),
            "owner": record.get("owner"),
            "is_admin": record.get("is_admin"),
            "identifier": record.get("identifier"),
            "pv_max": record.get("pvMax"),
            "has_mains": record.get("hasMains"),
            "has_generator": record.get("hasGenerator"),
            "alarm_monitoring": record.get("alarmMonitoring"),
            "currency_code": record.get("currencyCode"),
            "device_icon": record.get("device_icon")
        }

        site_record = {
            "id_site": site_id,
            "primary_function": "industrial",  # Default for Voltcore installations
            "dis": record.get("name"),
            "geo_addr": None,
            "geo_street": None,
            "geo_city": None,
            "geo_coord": geo_coord,
            "geo_country": None,
            "geo_county": None,
            "geo_postal_code": None,
            "geo_state": None,
            "area": None,
            "tz": record.get("timezone"),
            "year_built": None,
            "connector": "voltcore_api",
            "source_site_id": str(record["idSite"]),
            "other": json.dumps(other_fields)
        }

        voltcore_sites.append(site_record)
        print(f"  ✓ Added site: {record.get('name')} (ID: {site_id})")

    print(f"\n✓ Total Voltcore sites: {len(voltcore_sites)}")

except Exception as e:
    print(f"✗ Error fetching Voltcore sites: {e}")

# COMMAND ----------

# DBTITLE 1,Fetch Sunpeak Sites
print("\n" + "="*60)
print("FETCHING SUNPEAK SITES")
print("="*60)

sunpeak_sites = []

try:
    params = {"api_key": SUNPEAK_API_KEY}

    sites_response = requests.get(SUNPEAK_SITES_URL, params=params)
    sites_response.raise_for_status()

    sites_data = sites_response.json()["sites"]["site"]

    for site in sites_data:
        site_id = f"sunpeak-{site['id']}"

        location = site.get("location", {})

        # Parse coordinates
        geo_coord = None
        # Sunpeak doesn't provide coordinates in sites/list endpoint

        # Infer primary function from site type
        site_type = site.get("type", "")
        if "Residential" in site_type:
            primary_function = "residential"
        elif "Commercial" in site_type:
            primary_function = "commercial"
        else:
            primary_function = "industrial"

        # Store additional fields
        other_fields = {
            "account_id": site.get("accountId"),
            "status": site.get("status"),
            "peak_power": site.get("peakPower"),
            "last_update_time": site.get("lastUpdateTime"),
            "currency": site.get("currency"),
            "installation_date": site.get("installationDate"),
            "pto_date": site.get("ptoDate"),
            "notes": site.get("notes"),
            "type": site.get("type")
        }

        site_record = {
            "id_site": site_id,
            "primary_function": primary_function,
            "dis": site.get("name"),
            "geo_addr": location.get("address"),
            "geo_street": location.get("address"),
            "geo_city": location.get("city"),
            "geo_coord": geo_coord,
            "geo_country": location.get("countryCode"),
            "geo_county": None,
            "geo_postal_code": location.get("zip"),
            "geo_state": None,
            "area": None,
            "tz": location.get("timeZone"),
            "year_built": None,
            "connector": "sunpeak_api",
            "source_site_id": str(site["id"]),
            "other": json.dumps(other_fields)
        }

        sunpeak_sites.append(site_record)
        print(f"  ✓ Added site: {site.get('name')} (ID: {site_id})")

    print(f"\n✓ Total Sunpeak sites: {len(sunpeak_sites)}")

except Exception as e:
    print(f"✗ Error fetching Sunpeak sites: {e}")

# COMMAND ----------

# DBTITLE 1,Fetch Solarflow Sites
print("\n" + "="*60)
print("FETCHING SOLARFLOW SITES")
print("="*60)

solarflow_sites = []

try:
    import solarflowServer

    api = solarflowServer.OpenApiV1(token=SOLARFLOW_API_KEY)
    plants = api.plant_list()

    for plant in plants["plants"]:
        site_id = f"solarflow-{plant['plant_id']}"

        # Parse coordinates
        latitude = plant.get("latitude")
        longitude = plant.get("longitude")
        geo_coord = f"C({latitude},{longitude})" if latitude and longitude else None

        # Store additional fields
        other_fields = {
            "total_energy": plant.get("total_energy"),
            "current_power": plant.get("current_power"),
            "locale": plant.get("locale"),
            "peak_power": plant.get("peak_power"),
            "operator": plant.get("operator"),
            "installer": plant.get("installer"),
            "user_id": plant.get("user_id"),
            "create_date": plant.get("create_date"),
            "status": plant.get("status"),
            "image_url": plant.get("image_url")
        }

        site_record = {
            "id_site": site_id,
            "primary_function": "industrial",  # Default for Solarflow plants
            "dis": plant.get("name"),
            "geo_addr": None,
            "geo_street": None,
            "geo_city": plant.get("city"),
            "geo_coord": geo_coord,
            "geo_country": plant.get("country"),
            "geo_county": None,
            "geo_postal_code": None,
            "geo_state": None,
            "area": None,
            "tz": None,  # Solarflow doesn't provide timezone
            "year_built": None,
            "connector": "solarflow_api",
            "source_site_id": str(plant["plant_id"]),
            "other": json.dumps(other_fields)
        }

        solarflow_sites.append(site_record)
        print(f"  ✓ Added site: {plant.get('name')} (ID: {site_id})")

    print(f"\n✓ Total Solarflow sites: {len(solarflow_sites)}")

except Exception as e:
    print(f"✗ Error fetching Solarflow sites: {e}")

# COMMAND ----------

# DBTITLE 1,Combine All Sites and Create DataFrame
print("\n" + "="*60)
print("PROCESSING SITES DATA")
print("="*60)

all_sites = voltcore_sites + sunpeak_sites + solarflow_sites

print(f"Total sites collected: {len(all_sites)}")
print(f"  - Voltcore: {len(voltcore_sites)}")
print(f"  - Sunpeak: {len(sunpeak_sites)}")
print(f"  - Solarflow: {len(solarflow_sites)}")

if len(all_sites) == 0:
    raise Exception("No sites collected from any API!")

# Define schema
site_schema = StructType([
    StructField("connector", StringType(), True),
    StructField("source_site_id", StringType(), True),
    StructField("id_site", StringType(), True),
    StructField("primary_function", StringType(), True),
    StructField("dis", StringType(), True),
    StructField("geo_addr", StringType(), True),
    StructField("geo_street", StringType(), True),
    StructField("geo_city", StringType(), True),
    StructField("geo_coord", StringType(), True),
    StructField("geo_country", StringType(), True),
    StructField("geo_county", StringType(), True),
    StructField("geo_postal_code", StringType(), True),
    StructField("geo_state", StringType(), True),
    StructField("area", StringType(), True),
    StructField("tz", StringType(), True),
    StructField("year_built", StringType(), True),
    StructField("other", StringType(), True)
])

sites_df = spark.createDataFrame(all_sites, schema=site_schema)

# Generate surrogate key: MD5(connector + source_site_id)
sites_df = sites_df.withColumn(
    "sk_site",
    F.md5(F.concat(F.col("connector"), F.lit("_"), F.col("source_site_id")))
)

# Cast numeric fields
sites_df = sites_df.withColumn("area", F.col("area").cast(DecimalType(10, 2)))
sites_df = sites_df.withColumn("year_built", F.col("year_built").cast(IntegerType()))

# Add metadata timestamp
sites_df = sites_df.withColumn("metadata_updated_at", F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Merge Sites into dim_site
sites_df.createOrReplaceTempView("sites_staging")

merge_sql = f"""
MERGE INTO {dim_site_table} AS target
USING sites_staging AS source
ON target.sk_site = source.sk_site
WHEN MATCHED THEN
  UPDATE SET
    target.id_site = source.id_site,
    target.primary_function = source.primary_function,
    target.dis = source.dis,
    target.geo_addr = source.geo_addr,
    target.geo_street = source.geo_street,
    target.geo_city = source.geo_city,
    target.geo_coord = source.geo_coord,
    target.geo_country = source.geo_country,
    target.geo_county = source.geo_county,
    target.geo_postal_code = source.geo_postal_code,
    target.geo_state = source.geo_state,
    target.area = source.area,
    target.tz = source.tz,
    target.year_built = source.year_built,
    target.connector = source.connector,
    target.source_site_id = source.source_site_id,
    target.other = source.other,
    target.metadata_updated_at = source.metadata_updated_at
WHEN NOT MATCHED THEN
  INSERT (
    sk_site, id_site, primary_function, dis, geo_addr, geo_street, geo_city, geo_coord,
    geo_country, geo_county, geo_postal_code, geo_state, area, tz, year_built,
    connector, source_site_id, other, metadata_updated_at
  )
  VALUES (
    source.sk_site, source.id_site, source.primary_function, source.dis, source.geo_addr, source.geo_street,
    source.geo_city, source.geo_coord, source.geo_country, source.geo_county,
    source.geo_postal_code, source.geo_state, source.area, source.tz, source.year_built,
    source.connector, source.source_site_id, source.other, source.metadata_updated_at
  )
"""

spark.sql(merge_sql)

total_sites = spark.table(dim_site_table).count()
print(f"\n✓ Successfully merged {len(all_sites)} sites into {dim_site_table}")
print(f"  Total sites in table: {total_sites}")

# COMMAND ----------

# DBTITLE 1,Generate Default Spaces (One per Site)
print("\n" + "="*60)
print("GENERATING DEFAULT SPACES")
print("="*60)

# Create one default space per site
spaces_df = sites_df.select(
    F.col("sk_site"),
    F.col("id_site"),
    F.concat(F.col("id_site"), F.lit("-space-default")).alias("id_space"),
    F.concat(F.col("dis"), F.lit(" - Default Space")).alias("dis"),
    F.lit(None).cast(DecimalType(10, 2)).alias("area"),
    F.col("connector"),
    F.lit(None).cast(StringType()).alias("other"),
    F.current_timestamp().alias("metadata_updated_at")
)

# Generate surrogate key for space: MD5(sk_site + "default")
spaces_df = spaces_df.withColumn(
    "sk_space",
    F.md5(F.concat(F.col("sk_site"), F.lit("_default")))
)

print(f"Generated {spaces_df.count()} default spaces (one per site)")

# COMMAND ----------

# DBTITLE 1,Merge Spaces into dim_space
spaces_df.createOrReplaceTempView("spaces_staging")

merge_sql = f"""
MERGE INTO {dim_space_table} AS target
USING spaces_staging AS source
ON target.sk_space = source.sk_space
WHEN MATCHED THEN
  UPDATE SET
    target.id_space = source.id_space,
    target.sk_site = source.sk_site,
    target.id_site = source.id_site,
    target.dis = source.dis,
    target.area = source.area,
    target.connector = source.connector,
    target.other = source.other,
    target.metadata_updated_at = source.metadata_updated_at
WHEN NOT MATCHED THEN
  INSERT (
    sk_space, id_space, sk_site, id_site, dis, area, connector, other, metadata_updated_at
  )
  VALUES (
    source.sk_space, source.id_space, source.sk_site, source.id_site, source.dis, source.area,
    source.connector, source.other, source.metadata_updated_at
  )
"""

spark.sql(merge_sql)

total_spaces = spark.table(dim_space_table).count()
print(f"\n✓ Successfully merged {spaces_df.count()} spaces into {dim_space_table}")
print(f"  Total spaces in table: {total_spaces}")

# COMMAND ----------

# DBTITLE 1,Show Sites Summary
print("\n" + "="*60)
print("SITES SUMMARY")
print("="*60)

spark.sql(f"""
    SELECT
        connector,
        COUNT(*) as site_count,
        COUNT(DISTINCT geo_country) as countries,
        COUNT(DISTINCT geo_city) as cities
    FROM {dim_site_table}
    GROUP BY connector
    ORDER BY connector
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Sites Details
print("\nSites Details:")
spark.sql(f"""
    SELECT
        id_site,
        dis as site_name,
        primary_function,
        geo_city,
        geo_country,
        tz,
        connector
    FROM {dim_site_table}
    ORDER BY connector, id_site
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Spaces Summary
print("\n" + "="*60)
print("SPACES SUMMARY")
print("="*60)

spark.sql(f"""
    SELECT
        connector,
        COUNT(*) as space_count
    FROM {dim_space_table}
    GROUP BY connector
    ORDER BY connector
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Spaces Details
print("\nSpaces Details:")
spark.sql(f"""
    SELECT
        id_space,
        id_site,
        dis as space_name,
        connector
    FROM {dim_space_table}
    ORDER BY connector, id_space
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Verify Referential Integrity
print("\n" + "="*60)
print("REFERENTIAL INTEGRITY CHECK")
print("="*60)

orphaned_spaces = spark.sql(f"""
    SELECT COUNT(*) as orphaned_count
    FROM {dim_space_table} s
    LEFT JOIN {dim_site_table} site ON s.sk_site = site.sk_site
    WHERE site.sk_site IS NULL
""").collect()[0]["orphaned_count"]

if orphaned_spaces > 0:
    print(f"⚠ WARNING: Found {orphaned_spaces} orphaned spaces without matching sites!")
else:
    print(f"✓ All spaces have valid site references")

print("\n✓ Site and Space metadata refresh completed successfully!")
