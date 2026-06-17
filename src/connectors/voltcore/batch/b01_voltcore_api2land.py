# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import os
import requests
from datetime import datetime

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "voltcore_inverters_batch", "Volume name for the API files")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

USERNAME = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

LOGIN_URL = "https://vrmapi.voltcoreenergy.com/v2/auth/login"
USER_INSTALLATIONS_URL = "https://vrmapi.voltcoreenergy.com/v2/users/{idUser}/installations"
INSTALLATION_DATA_URL = "https://vrmapi.voltcoreenergy.com/v2/installations/{idSite}/data-download"
SYSTEM_OVERVIEW_URL = "https://vrmapi.voltcoreenergy.com/v2/installations/{idSite}/system-overview"

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw CSV files from Voltcore API'
""")

# COMMAND ----------

# DBTITLE 1,Login and Get Bearer Token
login_payload = {
    "username": USERNAME,
    "password": PASSWORD,
    "remember_me": False
}

login_response = requests.post(LOGIN_URL, json=login_payload)
login_response.raise_for_status()

token_data = login_response.json()
bearer_token = token_data["token"]
user_id = token_data["idUser"]

headers = {
    "x-authorization": f"Bearer {bearer_token}"
}

# COMMAND ----------

# DBTITLE 1,Fetch All User Installations
installations_url = USER_INSTALLATIONS_URL.format(idUser=user_id)
installations_response = requests.get(installations_url, headers=headers)
installations_response.raise_for_status()

installations_data = installations_response.json()
site_ids = [record["idSite"] for record in installations_data["records"]]
site_names = {record["idSite"]: record["name"] for record in installations_data["records"]}

print(f"Found {len(site_ids)} installations:")
for site_id in site_ids:
    print(f"  - Site ID {site_id}: {site_names[site_id]}")

# COMMAND ----------

# DBTITLE 1,Download Data from All Sites
params = {
    "async": "false",
    "datatype": "log",
    "format": "csv"
}

total_files_saved = 0
successful_sites = []
failed_sites = []

for site_id in site_ids:
    try:
        print(f"\n--- Processing Site ID: {site_id} ({site_names[site_id]}) ---")

        # First, get system overview to extract productCode for VE.Bus System (inverter)
        overview_url = SYSTEM_OVERVIEW_URL.format(idSite=site_id)
        overview_response = requests.get(overview_url, headers=headers)
        overview_response.raise_for_status()

        overview_data = overview_response.json()

        # Find the VE.Bus System device (inverter) and extract productCode
        product_code = None
        devices = overview_data.get("records", {}).get("devices", [])
        for device in devices:
            if device.get("name") == "VE.Bus System":
                product_code = device.get("productCode")
                product_name = device.get("productName", "Unknown")
                print(f"  Found inverter: {product_name} (Product Code: {product_code})")
                break

        if not product_code:
            print(f"  Warning: No VE.Bus System (inverter) found for site {site_id}. Using site_id as product_code.")
            product_code = str(site_id)

        # Now fetch the data download
        formatted_url = INSTALLATION_DATA_URL.format(idSite=site_id)
        response = requests.get(formatted_url, params=params, headers=headers)
        response.raise_for_status()

        response_data = response.text.strip().strip("'\"")

        # Check if we got actual data
        if not response_data or len(response_data) < 100:
            print(f"  Warning: Site {site_id} returned no data or very little data. Skipping.")
            failed_sites.append({"site_id": site_id, "reason": "no_data"})
            continue

        # Generate filename with timestamp, site_id, and product_code
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"voltcore_site_{site_id}_device_{product_code}_timestamp_{timestamp}.csv"
        file_path = os.path.join(VOLUME_PATH, filename)

        # Save the CSV content to volume
        print(f"Saving data to: {file_path}")
        with open(file_path, 'w') as f:
            f.write(response_data)

        print(f"Successfully saved data from site {site_id}")
        total_files_saved += 1
        successful_sites.append(site_id)

    except requests.exceptions.HTTPError as e:
        print(f"HTTP error fetching data from site {site_id}: {e}")
        if e.response is not None:
            print(f"Response status: {e.response.status_code}, Response body: {e.response.text}")
        failed_sites.append({"site_id": site_id, "reason": str(e)})

    except Exception as e:
        print(f"Error processing site {site_id}: {e}")
        failed_sites.append({"site_id": site_id, "reason": str(e)})
