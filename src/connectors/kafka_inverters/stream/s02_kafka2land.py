# Databricks notebook source
# DBTITLE 1,Requirements Install
# MAGIC %pip install \
# MAGIC   avro==1.11.3 \
# MAGIC   certifi==2023.7.22 \
# MAGIC   charset-normalizer==3.3.0 \
# MAGIC   confluent-kafka==2.2.0 \
# MAGIC   fastavro==1.8.3 \
# MAGIC   idna==3.4 \
# MAGIC   requests==2.31.0 \
# MAGIC   urllib3==2.0.6

# COMMAND ----------

# DBTITLE 1,Restart the Kernel
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import json
import requests
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.error import SerializationError

# --- Unity Catalog configuration ---
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for Kafka credentials")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

# --- Kafka credentials ---
# user specific details
KAFKA_GLOBEX_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_api_key")
KAFKA_GLOBEX_API_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_api_password")
KAFKA_GLOBEX_CONSUMER_GROUP = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_consumer_group")

# Kafka details
KAFKA_GLOBEX_BOOTSTRAP_SERVERS = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_bootstrap_servers")
KAFKA_GLOBEX_SCHEMA_REGISTRY_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_schema_registry_url")
KAFKA_GLOBEX_SCHEMA_REGISTRY_USERNAME = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_schema_registry_username")
KAFKA_GLOBEX_SCHEMA_REGISTRY_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_schema_registry_password")

#Project specific details
KAFKA_GLOBEX_CLIENT_ID_CONSUMER = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_client_id_consumer")
KAFKA_GLOBEX_TOPIC = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka_globex_topic")

dbutils.widgets.text("volume_name", "kafka_inverters_stream", "Volume name for the Kafka JSON data")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

dbutils.widgets.text("dlq_volume_name", "kafka_inverters_dlq", "DLQ volume name for failed Kafka messages")
DLQ_VOLUME_NAME = dbutils.widgets.get("dlq_volume_name")
DLQ_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/operational/{DLQ_VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Create Volumes if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON batches from Kafka inverters stream'
""")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.operational.{DLQ_VOLUME_NAME}
    COMMENT 'Dead Letter Queue volume for failed Kafka messages that could not be deserialized'
""")

# COMMAND ----------

# DBTITLE 1,Schema Registry for Avro Deserializing
# SchemaRegistryClient will be needed to instantiate AvroDeserializer.
conf = {
    'url': KAFKA_GLOBEX_SCHEMA_REGISTRY_URL,
    'basic.auth.user.info': f"{KAFKA_GLOBEX_SCHEMA_REGISTRY_USERNAME}:{KAFKA_GLOBEX_SCHEMA_REGISTRY_PASSWORD}"
}
schema_registry_client = SchemaRegistryClient(conf)

# Get the schema from the Schema Registry.
r = requests.get(
    f"{KAFKA_GLOBEX_SCHEMA_REGISTRY_URL}/subjects/{KAFKA_GLOBEX_TOPIC}-value/versions/latest",
    auth=(KAFKA_GLOBEX_SCHEMA_REGISTRY_USERNAME, KAFKA_GLOBEX_SCHEMA_REGISTRY_PASSWORD)
)
print(r.status_code)
schema_str = r.json()['schema']
parsed_schema = json.loads(schema_str)
print(json.dumps(parsed_schema, indent=4))

# COMMAND ----------

# DBTITLE 1,Helper Function to Write to Volume
def write_batch_to_volume(values):
    """
    Write a batch of messages to Unity Catalog Volume
    """
    if not values:
        return None

    # Generate unique filename with timestamp
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
    filename = f"kafka_batch_{timestamp}.json"
    file_path = f"{VOLUME_PATH}/{filename}"

    # Prepare data with metadata
    batch_data = {
        "ingestion_timestamp": now.isoformat(),
        "source_topic": KAFKA_GLOBEX_TOPIC,
        "consumer_group": KAFKA_GLOBEX_CONSUMER_GROUP,
        "message_count": len(values),
        "messages": values
    }

    # Write to volume
    json_content = json.dumps(batch_data, indent=2, default=str)
    dbutils.fs.put(file_path, json_content, overwrite=False)

    print(f"Written {len(values)} messages to {file_path}")
    return file_path

# COMMAND ----------

# DBTITLE 1,DLQ Helper Function
def write_to_dlq(failed_messages, error_type, error_details):
    """
    Write failed messages to Dead Letter Queue with error metadata

    Args:
        failed_messages: List of failed message data (can be raw bytes, partial objects, etc.)
        error_type: Type of error (e.g., 'serialization_error', 'kafka_error', 'processing_error')
        error_details: Detailed error information
    """
    if not failed_messages:
        return None

    # Generate unique filename with timestamp
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
    filename = f"dlq_{error_type}_{timestamp}.json"
    file_path = f"{DLQ_VOLUME_PATH}/{filename}"

    # Prepare DLQ data with error metadata
    dlq_data = {
        "dlq_timestamp": now.isoformat(),
        "error_type": error_type,
        "error_details": str(error_details),
        "source_topic": KAFKA_GLOBEX_TOPIC,
        "consumer_group": KAFKA_GLOBEX_CONSUMER_GROUP,
        "failed_message_count": len(failed_messages),
        "failed_messages": []
    }

    # Process failed messages - handle different types of failures
    for i, msg in enumerate(failed_messages):
        failed_msg_data = {
            "message_index": i,
            "failure_timestamp": now.isoformat()
        }

        # Handle different message types
        if hasattr(msg, 'value') and hasattr(msg, 'key'):
            # Kafka message object
            try:
                failed_msg_data.update({
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1] if msg.timestamp()[0] != -1 else None,
                    "key": msg.key().decode('utf-8') if msg.key() else None,
                    "value_raw": msg.value().hex() if msg.value() else None,  # Store as hex for binary safety
                    "headers": dict(msg.headers()) if msg.headers() else {}
                })
            except Exception as e:
                failed_msg_data["extraction_error"] = str(e)
                failed_msg_data["raw_message"] = str(msg)
        else:
            # Other types of failed data
            failed_msg_data["raw_data"] = str(msg)

        dlq_data["failed_messages"].append(failed_msg_data)

    # Write to DLQ volume
    json_content = json.dumps(dlq_data, indent=2, default=str)
    dbutils.fs.put(file_path, json_content, overwrite=False)

    print(f"Written {len(failed_messages)} failed messages to DLQ: {file_path}")
    return file_path

# COMMAND ----------

# DBTITLE 1,Kafka Stream and Write to Volume
# AvroDeserializer and SerializationContext will be needed to deserialize individual messages when a regular Consumer is used.
deserializer = AvroDeserializer(schema_registry_client, schema_str)
ser_context = SerializationContext(KAFKA_GLOBEX_TOPIC, MessageField.VALUE)

# Consumer configuration. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {
    # Choose a unique consumer group id and a client id.
    'group.id': KAFKA_GLOBEX_CONSUMER_GROUP,
    'client.id': KAFKA_GLOBEX_CLIENT_ID_CONSUMER,
    'bootstrap.servers': KAFKA_GLOBEX_BOOTSTRAP_SERVERS,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': KAFKA_GLOBEX_API_KEY,
    'sasl.password': KAFKA_GLOBEX_API_PASSWORD,
    'auto.offset.reset': 'latest',
    'enable.auto.commit': 'false'
}

# Create a Consumer instance.
consumer = Consumer(conf)

def print_assignment(consumer, partitions):
    print(f'Assignment: {partitions}')

def no_offset_assignment(consumer, partitions):
    for p in partitions:
        p.offset = -1
    print('Assignment: ', partitions)
    consumer.assign(partitions)

# --- Subscribe to the TOPIC. ---
# If you just want to consume the most recent message, use on_assign=no_offset_assignment.
# If you want to continue where you left off, use on_assign=print_assignment.
consumer.subscribe([KAFKA_GLOBEX_TOPIC], on_assign=no_offset_assignment)

# Read messages from Kafka.
try:
    while True:
        try:
            # Instead of using poll that returns messages one-by-one, we use consume to receive a list of messages.
            # In this case, we receive a list of messages every time there are 1000 new messages or 5 seconds elapse.
            msgs = consumer.consume(num_messages=1000, timeout=5.0)
            print(f"Received {len(msgs)} messages.")

        except KafkaError as e:
            print("KafkaError occurred, no offset will be committed: {}".format(e))
            # Write Kafka error to DLQ
            write_to_dlq([{"kafka_error": str(e)}], "kafka_error", e)
            continue

        # Loop over the batch of messages obtained from Kafka. Individual messages will be deserialized.
        # Messages that cannot be deserialized will be sent to DLQ.
        # In case any other errors from Kafka occur, no offset will be commited to Kafka.
        msg_err = False
        values = []
        failed_messages = []

        for msg in msgs:

            if msg.error():
                # On error, send to DLQ and do not commit any offset to Kafka.
                failed_messages.append(msg)
                msg_err = True
                print(f"Message error occurred: {msg.error()}")
                continue

            try:
                value = deserializer(msg.value(), ser_context)
                values.append(value)

            except SerializationError as e:
                # Send failed serialization messages to DLQ and continue processing other messages
                print("SerializationError occurred, the message will be sent to DLQ: {}".format(e))
                failed_messages.append(msg)
                continue
            except Exception as e:
                # Catch any other deserialization errors
                print("Unexpected deserialization error occurred, the message will be sent to DLQ: {}: {}".format(type(e).__name__, e))
                failed_messages.append(msg)
                continue

        # Handle failed messages by writing to DLQ
        if failed_messages:
            # Determine error type based on the nature of failures
            if msg_err:
                dlq_file = write_to_dlq(failed_messages, "message_error", "Kafka message contained error")
            else:
                dlq_file = write_to_dlq(failed_messages, "serialization_error", "Message deserialization failed")
            print(f"DLQ processing completed: {dlq_file}")

        # On critical message error, do not commit any offset to Kafka and try again.
        if msg_err:
            continue

        # Write batch to volume with date partitioning
        if values:
            volume_file = write_batch_to_volume(values)
        else:
            print("No successful messages to write to volume in this batch")

        # Commit the last offset to Kafka on success.
        # This includes successful processing of good messages and successful DLQ handling of bad messages
        consumer.commit()

finally:
    consumer.close()