# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import smtplib
from datetime import datetime
from email.message import EmailMessage

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("silver_table_name", "kafka_inverters_stream", "Silver table to monitor")
SILVER_TABLE_NAME = dbutils.widgets.get("silver_table_name")
SILVER_PATH = f"{CATALOG_NAME}.silver.{SILVER_TABLE_NAME}"

dbutils.widgets.text("state_table_name", "kafka_inverters_heartbeat_state_table", "Source data monitor table name")
SOURCE_DATA_MONITORING_TABLE_NAME = dbutils.widgets.get("state_table_name")
SOURCE_DATA_MONITORING_PATH = f"{CATALOG_NAME}.operational.{SOURCE_DATA_MONITORING_TABLE_NAME}"

CHECKPOINT_LOCATION = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/{SILVER_TABLE_NAME}_data_source_monitoring"

# Monitoring configuration
MONITORING_INTERVAL_MINUTES = 5
ALERT_THRESHOLD_MINUTES = 15

dbutils.widgets.text("recipient_emails", "developer@example.com", "Comma-separated recipient emails")
RECIPIENT_EMAILS = dbutils.widgets.get("recipient_emails").split(",")

# COMMAND ----------

# DBTITLE 1,Create Source Data Monitoring Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SOURCE_DATA_MONITORING_PATH} (
    monitor_id STRING COMMENT 'Identifier for the monitor',
    last_alert_timestamp TIMESTAMP COMMENT 'When the last alert was sent',
    alert_active BOOLEAN COMMENT 'Is there an active unresolved alert',
    last_data_timestamp TIMESTAMP COMMENT 'Last time we saw data (for context)',
    PRIMARY KEY (monitor_id)
)
COMMENT 'Manage alert state to prevent duplicate emails.'
""")

# Initialize state if not exists
existing = spark.sql(f"""
    SELECT * FROM {SOURCE_DATA_MONITORING_PATH} 
    WHERE monitor_id = '{SILVER_PATH}'
""").first()

if not existing:
    spark.sql(f"""
        INSERT INTO {SOURCE_DATA_MONITORING_PATH} VALUES
        ('{SILVER_PATH}', null, false, current_timestamp())
    """)

# COMMAND ----------

# DBTITLE 1,Email Alert Function
def send_data_gap_alert(gap_minutes, last_data_time):
    """Send alert email when data ingestion gap is detected"""
    
    smtp_user = dbutils.secrets.get(scope="aws_ses", key="smtp_user_name")
    smtp_password = dbutils.secrets.get(scope="aws_ses", key="smtp_password")
    smtp_host = "email-smtp.eu-central-1.amazonaws.com"
    smtp_port = 587
    
    sender_email = "notifications@dataplatformx.com"
    
    subject = f"Data Ingestion Alert: {SILVER_TABLE_NAME}"
    
    body = f"""
    Data ingestion gap detected in the streaming pipeline.
    
    Table: {SILVER_PATH}
    Last Data Received: {last_data_time}
    Gap Duration: {gap_minutes:.1f} minutes
    Alert Threshold: {ALERT_THRESHOLD_MINUTES} minutes
    Current Time: {datetime.now()}
    
    Action Required:
    - Check Kafka consumer status and connectivity
    - Verify source system is producing data
    - Check Bronze to Silver streaming job status
    - Review Databricks job logs for errors
    
    Note: This is a one-time alert for this incident. You will receive another alert only after data resumes and a new incident occurs.
    """
    
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(RECIPIENT_EMAILS)
    
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        print(f"Alert email sent - Gap: {gap_minutes:.1f} minutes")
        return True
    except Exception as e:
        print(f"Failed to send alert email: {e}")
        return False

# COMMAND ----------

# DBTITLE 1,Monitoring Logic
def check_data_ingestion():
    """Check if Silver table has recent data and send alert if needed."""
    
    current_time = datetime.now()
    
    # Get latest data from Silver table
    try:
        latest_record = spark.sql(f"""
            SELECT MAX(silver_processing_timestamp) as max_timestamp
            FROM {SILVER_PATH}
            WHERE silver_processing_timestamp > current_timestamp() - INTERVAL 1 HOUR
        """).first()
        
        latest_data_time = latest_record.max_timestamp
    except Exception as e:
        print(f"Error querying Silver table: {e}")
        return
    
    if not latest_data_time:
        # No data in the last hour, check without time filter
        try:
            latest_record = spark.sql(f"""
                SELECT MAX(silver_processing_timestamp) as max_timestamp
                FROM {SILVER_PATH}
            """).first()
            latest_data_time = latest_record.max_timestamp
        except:
            print("Could not retrieve latest timestamp from Silver table")
            return
    
    if latest_data_time:
        # Calculate data gap
        gap = current_time - latest_data_time.replace(tzinfo=None)
        gap_minutes = gap.total_seconds() / 60
        
        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Last data: {gap_minutes:.1f} minutes ago")
        
        # Get current alert state
        alert_state = spark.sql(f"""
            SELECT * FROM {SOURCE_DATA_MONITORING_PATH}
            WHERE monitor_id = '{SILVER_PATH}'
        """).first()
        
        if gap_minutes > ALERT_THRESHOLD_MINUTES:
            # Data gap detected
            if not alert_state.alert_active:
                # No active alert - send one email
                if send_data_gap_alert(gap_minutes, latest_data_time):
                    # Update alert state
                    spark.sql(f"""
                        UPDATE {SOURCE_DATA_MONITORING_PATH}
                        SET last_alert_timestamp = current_timestamp(),
                            alert_active = true,
                            last_data_timestamp = timestamp'{latest_data_time}'
                        WHERE monitor_id = '{SILVER_PATH}'
                    """)
                    print(f"ALERT SENT - Data gap of {gap_minutes:.1f} minutes detected")
            else:
                # Alert already active - do nothing (no duplicate emails)
                print(f"Data gap continues ({gap_minutes:.1f} minutes) - alert already sent")
        
        else:
            # Data is flowing normally
            if alert_state.alert_active:
                # Clear the alert state (data has resumed)
                spark.sql(f"""
                    UPDATE {SOURCE_DATA_MONITORING_PATH}
                    SET alert_active = false,
                        last_data_timestamp = timestamp'{latest_data_time}'
                    WHERE monitor_id = '{SILVER_PATH}'
                """)
                print("Data flow resumed - alert cleared")
            
            # Update last data timestamp
            spark.sql(f"""
                UPDATE {SOURCE_DATA_MONITORING_PATH}
                SET last_data_timestamp = timestamp'{latest_data_time}'
                WHERE monitor_id = '{SILVER_PATH}'
            """)

# COMMAND ----------

# DBTITLE 1,Streaming Monitor Function
def process_monitoring_batch(batch_df, batch_id):
    """Process function for streaming monitoring"""
    check_data_ingestion()

# COMMAND ----------

# DBTITLE 1,Start Streaming Monitor
monitoring_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .option("version", "2")  # Required for Databricks Runtime 17.x compatibility
    .load()
    .writeStream
    .foreachBatch(process_monitoring_batch)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime=f"{MONITORING_INTERVAL_MINUTES} minutes")
    .start()
)