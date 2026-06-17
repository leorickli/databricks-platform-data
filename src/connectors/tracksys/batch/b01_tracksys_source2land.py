# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import requests
import base64
import time
import zipfile
import io
import os
from datetime import datetime

TRACKSYS_API_URL = "https://newelectric-001.tracksolutions.com/api/v1/files/logger-data"

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "tracksys_batch", "Volume name for the API files")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

TRACKSYS_USERNAME = dbutils.secrets.get(scope=SECRET_SCOPE, key="tracksys_username")
TRACKSYS_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="tracksys_password")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw CSV and ASC files from TRACKSYS API'
""")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def delete_tracksys_file(relative_path_to_delete, auth, headers):
    """
    Sends a DELETE request to the TRACKSYS API for the given relativePath.
    Returns True if deletion was successful (or accepted), False otherwise.
    """
    if not relative_path_to_delete:
        print("No relativePath provided for deletion.")
        return False

    delete_payload = {"relativePath": relative_path_to_delete}
    print(f"Requesting to delete file from TRACKSYS server: {relative_path_to_delete}")

    try:
        response = requests.delete(TRACKSYS_API_URL, headers=headers, auth=auth, json=delete_payload)
        response.raise_for_status() 

        if response.status_code in [200, 204]:
            print(f"Successfully sent delete request for {relative_path_to_delete}. Server responded with {response.status_code}.")
            return True
        else:
            print(f"Delete request for {relative_path_to_delete} returned status {response.status_code}. Content: {response.text}")
            return False

    except requests.exceptions.HTTPError as e:
        print(f"API DELETE request HTTP error for {relative_path_to_delete}: {e}")
        if e.response is not None:
            print(f"Response status: {e.response.status_code}, Response body: {e.response.text}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"API DELETE request failed for {relative_path_to_delete}: {e}")
        return False

def process_and_save_file(relative_path, base64_content):
    """
    Decodes, processes, and saves file content to Databricks Volume.
    Returns the number of files successfully saved.
    """
    files_saved = 0
    
    # Handle the .ok file: it has no data and doesn't need to be saved.
    if relative_path.lower().endswith(".ok"):
        print(f"Received confirmation file: {relative_path}. No save needed.")
        return files_saved

    if not base64_content:
        print(f"Warning: Data file {relative_path} has empty content. Skipping save.")
        return files_saved

    try:
        decoded_data = base64.b64decode(base64_content)
        
        # Convert Windows path to Unix path
        file_path_base = relative_path.replace('\\', '/')
        
        if file_path_base.lower().endswith(".zip"):
            print(f"File {relative_path} is a zip archive. Extracting all contents.")
            with io.BytesIO(decoded_data) as zip_in_memory:
                with zipfile.ZipFile(zip_in_memory, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    if not file_list:
                        print(f"Error: Zip file {relative_path} is empty.")
                        return files_saved

                    # Get directory path from zip file path
                    directory_path = ''
                    if '/' in file_path_base:
                        directory_path = file_path_base.rsplit('/', 1)[0] + '/'
                    
                    for file_name_in_zip in file_list:
                        final_file_name = file_name_in_zip
                        
                        # If the file is a .trip.asc file, treat it as the main .asc file
                        if file_name_in_zip.lower().endswith('.trip.asc'):
                            print(f"Processing trip file '{file_name_in_zip}' as a data file.")
                            final_file_name = file_name_in_zip.lower().replace('.trip.asc', '.asc')
                        
                        unzipped_data = zip_ref.read(file_name_in_zip)
                        
                        # Construct full path in volume
                        full_volume_path = os.path.join(VOLUME_PATH, directory_path + final_file_name)
                        
                        # Ensure directory exists
                        os.makedirs(os.path.dirname(full_volume_path), exist_ok=True)
                        
                        # Save to volume
                        print(f"Saving extracted file '{file_name_in_zip}' as '{final_file_name}' to: {full_volume_path}")
                        with open(full_volume_path, 'wb') as f:
                            f.write(unzipped_data)
                        
                        print("Successfully saved file.")
                        files_saved += 1
        else:
            # For non-zip files, save directly
            full_volume_path = os.path.join(VOLUME_PATH, file_path_base)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(full_volume_path), exist_ok=True)
            
            print(f"Saving file to: {full_volume_path}")
            with open(full_volume_path, 'wb') as f:
                f.write(decoded_data)
            
            print(f"Successfully saved {relative_path} to volume.")
            files_saved += 1

    except zipfile.BadZipFile:
        print(f"Error: Bad zip file for {relative_path}. Skipping save.")
    except Exception as e:
        print(f"An error occurred during save or processing for {relative_path}: {e}")
    
    return files_saved

# COMMAND ----------

# DBTITLE 1,Main Ingestion Loop
def main_tracksys_pull_and_delete_job():
    """
    Main job function to pull files from the TRACKSYS API one by one,
    processing and then deleting each to advance the queue.
    """
    print("Starting TRACKSYS file pull and delete job...")
    
    # Authentication
    auth = (TRACKSYS_USERNAME, TRACKSYS_PASSWORD)
    headers = {"Content-Type": "application/json"}
    
    total_files_processed = 0
    total_files_deleted = 0

    while True:
        try:
            print("\nRequesting a new file from TRACKSYS API...")
            response = requests.post(TRACKSYS_API_URL, headers=headers, auth=auth, json={})
            response.raise_for_status() 
            
            response_data = response.json()

            if not response_data:
                print("No new file available from the API. Job finished.")
                break

            relative_path = response_data.get("relativePath")
            base64_content = response_data.get("data")

            if not relative_path:
                print("Received response with no relativePath. Exiting loop.")
                break

            print(f"Received file reference: {relative_path}")
            
            # Process and save the file content
            files_saved = process_and_save_file(relative_path, base64_content)
            total_files_processed += files_saved

            # Immediately delete the file from the server to advance the queue
            if delete_tracksys_file(relative_path, auth, headers):
                total_files_deleted += 1
            else:
                print(f"Failed to delete file {relative_path} from TRACKSYS server. It may be reprocessed.")

            time.sleep(1)

        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 404:
                print("API returned 404, assuming no new files available.")
                break
            else:
                print(f"API request HTTP error: {e}")
                raise
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}. Exiting loop.")
            raise
        except ValueError as e: 
            print(f"Error decoding JSON response from API: {e}. Exiting loop.")
            break
            
    print("\n--- TRACKSYS JOB COMPLETED ---")
    print(f"Total files processed and saved to volume: {total_files_processed}")
    print(f"Total files successfully requested for deletion from TRACKSYS: {total_files_deleted}")
    
    # Return metrics for job monitoring
    return {
        "files_processed": total_files_processed,
        "files_deleted": total_files_deleted,
        "timestamp": datetime.now().isoformat()
    }

# COMMAND ----------

# DBTITLE 1,Execute Main Job
try:
    job_results = main_tracksys_pull_and_delete_job()
    
except Exception as e:
    print(f"The ingestion job failed with a critical error: {e}")
    raise 