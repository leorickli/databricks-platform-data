# Databricks notebook source
# DBTITLE 1,Cleanup S07 Checkpoint
"""
This notebook cleans up the s07 checkpoint that has version 1 data
Run this once before running s07 on Runtime 17.x
"""

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

CHECKPOINT_LOCATION = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/kafka_inverters_stream_data_source_monitoring"

print(f"Deleting checkpoint: {CHECKPOINT_LOCATION}")

try:
    dbutils.fs.rm(CHECKPOINT_LOCATION, recurse=True)
    print("✅ Checkpoint deleted successfully")
except Exception as e:
    print(f"⚠️  Error: {e}")
    print("This is normal if the checkpoint doesn't exist")

print("\nYou can now run s07 - it will create a new checkpoint with version 2")
