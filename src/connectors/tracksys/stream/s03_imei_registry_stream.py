# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import json
import smtplib
from pyspark.sql.functions import col, count, flatten, collect_list, array_distinct, size, udf
from pyspark.sql.types import ArrayType, StringType
from datetime import datetime, timedelta
from email.message import EmailMessage

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "tracksys_stream", "Bronze table name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_TABLE = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

dbutils.widgets.text("imei_registry_table_name", "imei_signal_mappings_streaming", "IMEI signal mappings table name")
IMEI_REGISTRY_TABLE_NAME = dbutils.widgets.get("imei_registry_table_name")
IMEI_REGISTRY_TABLE = f"{CATALOG_NAME}.operational.{IMEI_REGISTRY_TABLE_NAME}"

dbutils.widgets.text("imei_state_table_name", "imei_signal_mappings_state_streaming", "IMEI signal mappings state table name")
IMEI_STATE_TABLE_NAME = dbutils.widgets.get("imei_state_table_name")
IMEI_STATE_TABLE = f"{CATALOG_NAME}.operational.{IMEI_STATE_TABLE_NAME}"

DETECTION_CHECKPOINT = f"s3://dpx-s3-dev/acme/checkpoints/stream/{IMEI_REGISTRY_TABLE_NAME}"

MIN_MESSAGES_FOR_DETECTION = 5
IMEI_INACTIVE_THRESHOLD_DAYS = 7

SIGNAL_MAPPINGS = {
    'soc': ['soc', 'bms_soc', 'libal_soc', 'bms_soc_1', 'libal_soc_1'],
    'voltage': ['voltage', 'core_voltage_level', 'bms_pack_voltage', 'libal_packcurrent', 'bms_packvoltage_1', 'libal_packcurrent_1', 'outputvoltage', 'output_v', 'voltage_level'],
    'current': ['current', 'core_current_level', 'bms_pack_current', 'libal_packcurrent', 'bms_pack_current_1', 'libal_packcurrent_1', 'outputcurrent', 'output_a', 'current_level']
}

# COMMAND ----------

# DBTITLE 1,Create Consolidated Tables
# Temporary state for detection in progress
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {IMEI_STATE_TABLE} (
    imei BIGINT COMMENT 'Device ID being learned',
    message_count INT COMMENT 'Messages seen from this device so far',
    detected_signals ARRAY<STRING> COMMENT 'List of signal names found',
    last_updated TIMESTAMP COMMENT 'When we last saw data from this device'
) COMMENT 'Operational state for IMEI signal mapping detection during streaming processing';
""")

# Main table with everything consolidated
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {IMEI_REGISTRY_TABLE} (
    imei BIGINT COMMENT 'International Mobile Equipment Identity of the data logger device',
    -- Signal mappings
    has_all_signals BOOLEAN COMMENT 'Do we have all 3 required signals?',
    soc_signal STRING COMMENT 'Which signal name means battery charge for this device',
    voltage_signal STRING COMMENT 'Which signal name means voltage for this device',
    current_signal STRING COMMENT 'Which signal name means current for this device',
    -- Activity tracking
    first_seen TIMESTAMP COMMENT 'When we first saw this device',
    last_seen TIMESTAMP COMMENT 'When we last saw this device',
    total_messages BIGINT COMMENT 'Total count of messages ever received',
    is_active BOOLEAN COMMENT 'Is the device currently sending data?',
    days_inactive INT COMMENT 'How many days since last data (0 if active)',
    -- Metadata
    detection_timestamp TIMESTAMP COMMENT 'When we figured out the signal mappings',
    last_activity_check TIMESTAMP COMMENT 'When we last checked if device is active'
) COMMENT 'Operational registry tracking IMEI signal mappings and activity status for streaming pipeline';
""")

# COMMAND ----------

# DBTITLE 1,Signal Extraction UDF
@udf(returnType=ArrayType(StringType()))
def extract_signal_names(signals_json_str):
    """Extract signal names from JSON"""
    try:
        if not signals_json_str:
            return []
        
        signals = json.loads(signals_json_str)
        if not isinstance(signals, list):
            return []
        
        signal_names = set()
        for signal in signals:
            if isinstance(signal, dict) and 'displayName' in signal:
                display_name = signal.get('displayName')
                if display_name:
                    signal_names.add(display_name.lower())
        
        return list(signal_names)
    except:
        return []

# COMMAND ----------

# DBTITLE 1,Email notification for new IMEIs
def send_new_imei_email(imei, mapping_details):
    """Sends an email notification for a newly detected IMEI using AWS SES."""
    
    # Securely retrieve credentials from Databricks Secrets
    smtp_user = dbutils.secrets.get(scope="aws_ses", key="smtp_user_name")
    smtp_password = dbutils.secrets.get(scope="aws_ses", key="smtp_password")
    smtp_host = "email-smtp.eu-central-1.amazonaws.com"
    smtp_port = 587
    
    recipient_email = "developer@example.com"
    sender_email = "notifications@dataplatformx.com"
    
    # Construct the email
    subject = f"New IMEI Detected: {imei}"
    body = f"""
    A new IMEI has been successfully mapped in the streaming pipeline.

    IMEI: {imei}
    SOC Signal: {mapping_details.get('soc', 'N/A')}
    Voltage Signal: {mapping_details.get('voltage', 'N/A')}
    Current Signal: {mapping_details.get('current', 'N/A')}
    """
    
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email

    # Send the email
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls() 
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            print(f"📧 Notification email sent for IMEI {imei} via AWS SES.")
    except Exception as e:
        print(f"Failed to send email for IMEI {imei} via AWS SES. Error: {e}")

# COMMAND ----------

# DBTITLE 1,Clean Up Orphaned State Records
def cleanup_orphaned_state_records():
    """Remove state records for IMEIs that are already in the registry"""
    
    # Get IMEIs that are in state table but already in registry
    orphaned_imeis = spark.sql(f"""
        SELECT DISTINCT state.imei
        FROM {IMEI_STATE_TABLE} state
        INNER JOIN {IMEI_REGISTRY_TABLE} registry
        ON state.imei = registry.imei
    """).collect()
    
    if orphaned_imeis:
        imei_list = [row.imei for row in orphaned_imeis]
        print(f"Cleaning up {len(imei_list)} orphaned state records: {imei_list}")
        
        for imei in imei_list:
            spark.sql(f"DELETE FROM {IMEI_STATE_TABLE} WHERE imei = {imei}")

# Run cleanup on notebook start
cleanup_orphaned_state_records()

# COMMAND ----------

# DBTITLE 1,Unified Detection and Activity Process
def process_detection_batch(batch_df, batch_id):
    """Process detection and update activity in one pass"""
    
    print(f"--- Detection Batch {batch_id} at {datetime.now()} ---")
    
    if batch_df.isEmpty():
        return
    
    # Get all IMEIs from this batch
    batch_imeis = (
        batch_df
        .select(col("logger_imei").cast("bigint").alias("imei"))
        .distinct()
        .collect()
    )
    batch_imei_list = [row.imei for row in batch_imeis]
    
    # Get already mapped IMEIs
    mapped_imeis_df = spark.sql(f"SELECT imei FROM {IMEI_REGISTRY_TABLE}")
    mapped_imeis = [row.imei for row in mapped_imeis_df.collect()]
    
    # Update last_seen for all IMEIs in batch (even if already mapped)
    for imei in batch_imei_list:
        if imei in mapped_imeis:
            # Update activity for existing IMEI
            message_count = batch_df.filter(col("logger_imei") == imei).count()
            
            spark.sql(f"""
                UPDATE {IMEI_REGISTRY_TABLE}
                SET last_seen = current_timestamp(),
                    total_messages = total_messages + {message_count},
                    is_active = true,
                    days_inactive = 0,
                    last_activity_check = current_timestamp()
                WHERE imei = {imei}
            """)
    
    # Check for inactive IMEIs (not in current batch)
    cutoff = datetime.now() - timedelta(days=IMEI_INACTIVE_THRESHOLD_DAYS)
    if batch_imei_list:  # Only update if we have IMEIs to exclude
        spark.sql(f"""
            UPDATE {IMEI_REGISTRY_TABLE}
            SET is_active = false,
                days_inactive = datediff(current_date(), last_seen),
                last_activity_check = current_timestamp()
            WHERE last_seen < '{cutoff}' 
            AND is_active = true
            AND imei NOT IN ({','.join(map(str, batch_imei_list))})
        """)
    
    # Process ONLY truly unmapped IMEIs for signal detection
    # Filter out IMEIs that are already in the registry
    unmapped_imeis = [imei for imei in batch_imei_list if imei not in mapped_imeis]
    
    # Also get IMEIs that are in detection state (but not yet mapped)
    detection_state_imeis = spark.sql(f"""
        SELECT DISTINCT imei 
        FROM {IMEI_STATE_TABLE}
        WHERE imei NOT IN (SELECT imei FROM {IMEI_REGISTRY_TABLE})
    """).collect()
    
    detection_imeis = [row.imei for row in detection_state_imeis]
    
    # Combine new unmapped IMEIs with those already in detection
    all_unmapped = list(set(unmapped_imeis + detection_imeis))
    
    if not all_unmapped:
        print("No unmapped IMEIs to process")
        return
    
    print(f"Processing {len(all_unmapped)} unmapped IMEIs: {all_unmapped}")
    
    # Extract signals for unmapped IMEIs only
    batch_signals = (
        batch_df
        .filter(col("logger_imei").isin(all_unmapped))
        .filter(col("signals").isNotNull())
        .select(
            col("logger_imei").cast("bigint").alias("imei"),
            extract_signal_names(col("signals")).alias("signal_names")
        )
        .filter(size(col("signal_names")) > 0)
        .groupBy("imei")
        .agg(
            flatten(collect_list("signal_names")).alias("all_signals"),
            count("*").alias("batch_messages")
        )
        .select(
            col("imei"),
            array_distinct(col("all_signals")).alias("detected_signals"),
            col("batch_messages")
        )
    )
    
    # Update detection state
    for row in batch_signals.collect():
        imei = row.imei
        new_signals = row.detected_signals
        new_messages = row.batch_messages
        
        # Double-check this IMEI isn't already mapped
        existing_mapping = spark.sql(f"""
            SELECT * FROM {IMEI_REGISTRY_TABLE} WHERE imei = {imei}
        """).first()
        
        if existing_mapping:
            print(f"⚠️ IMEI {imei} is already mapped, skipping detection")
            # Clean up any state record
            spark.sql(f"DELETE FROM {IMEI_STATE_TABLE} WHERE imei = {imei}")
            continue
        
        # Check if already in detection state
        existing_state = spark.sql(f"""
            SELECT * FROM {IMEI_STATE_TABLE} WHERE imei = {imei}
        """).first()
        
        if existing_state:
            # Update state
            combined_signals = list(set(existing_state.detected_signals + new_signals))
            new_count = existing_state.message_count + new_messages
            
            spark.sql(f"""
                UPDATE {IMEI_STATE_TABLE}
                SET detected_signals = array({','.join([f"'{s}'" for s in combined_signals])}),
                    message_count = {new_count},
                    last_updated = current_timestamp()
                WHERE imei = {imei}
            """)
            
            # Check if ready for mapping
            if new_count >= MIN_MESSAGES_FOR_DETECTION:
                # Try to map signals
                available_signals = set(combined_signals)
                mapping = {}
                
                for metric, candidates in SIGNAL_MAPPINGS.items():
                    found = False
                    for candidate in candidates:
                        if candidate in available_signals:
                            mapping[metric] = candidate
                            found = True
                            break
                    if not found:
                        mapping[metric] = "NOT FOUND"
                
                has_all = all(v != "NOT FOUND" for v in mapping.values())
                
                # Insert into main table
                spark.sql(f"""
                    INSERT INTO {IMEI_REGISTRY_TABLE}
                    VALUES (
                        {imei},
                        {has_all},
                        '{mapping.get('soc', 'NOT FOUND')}',
                        '{mapping.get('voltage', 'NOT FOUND')}',
                        '{mapping.get('current', 'NOT FOUND')}',
                        current_timestamp(),
                        current_timestamp(),
                        {new_count},
                        true,
                        0,
                        current_timestamp(),
                        current_timestamp()
                    )
                """)
                
                # Remove from state table
                spark.sql(f"DELETE FROM {IMEI_STATE_TABLE} WHERE imei = {imei}")
                
                print(f"✅ Mapped IMEI {imei}: SOC={mapping['soc']}, V={mapping['voltage']}, I={mapping['current']}")
                
                # Send notification email
                if has_all:
                    try:
                        send_new_imei_email(imei, mapping)
                    except Exception as e:
                        print(f"Failed to send email notification: {e}")
        else:
            # New IMEI - insert into state
            spark.sql(f"""
                INSERT INTO {IMEI_STATE_TABLE}
                VALUES (
                    {imei},
                    {new_messages},
                    array({','.join([f"'{s}'" for s in new_signals])}),
                    current_timestamp()
                )
            """)
            print(f"🆕 New IMEI detected: {imei}")

# COMMAND ----------

# DBTITLE 1,Start Stream
bronze_stream = spark.readStream.format("delta").table(BRONZE_TABLE)

detection_stream = (
    bronze_stream
    .writeStream
    .foreachBatch(process_detection_batch)
    .outputMode("update")
    .option("checkpointLocation", DETECTION_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)