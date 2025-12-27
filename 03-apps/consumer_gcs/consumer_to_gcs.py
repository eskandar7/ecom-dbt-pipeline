import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, TopicPartition
from google.cloud import storage

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecom_orders")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bronze-gcs-writer-dev")

BUCKET = os.getenv("GCS_BUCKET")
BASE_PREFIX = os.getenv("GCS_PREFIX", "bronze/orders")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))          # flush after N messages
FLUSH_SEC = int(os.getenv("FLUSH_SEC", "10"))            # or after N seconds
MAX_BYTES = int(os.getenv("MAX_BYTES", str(2 * 1024 * 1024)))  # or after ~2MB

if not BUCKET:
    raise SystemExit("GCS_BUCKET is required")

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    enable_auto_commit=False,          # commit only after upload success
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=1000,
)

buffer_lines = []
buffer_bytes = 0
last_flush = time.time()

def gcs_path_for_now():
    now = datetime.now(timezone.utc)
    dt = now.strftime("%Y-%m-%d")
    hr = now.strftime("%H")
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    return f"{BASE_PREFIX}/dt={dt}/hr={hr}/part-{ts}-{uuid.uuid4().hex}.jsonl"

def flush_to_gcs():
    global buffer_lines, buffer_bytes, last_flush
    if not buffer_lines:
        return

    path = gcs_path_for_now()
    blob = bucket.blob(path)

    payload = "\n".join(buffer_lines) + "\n"
    blob.upload_from_string(payload, content_type="application/x-ndjson")

    # Reset buffer only after successful upload
    count = len(buffer_lines)
    size = buffer_bytes
    buffer_lines = []
    buffer_bytes = 0
    last_flush = time.time()

    print(f"✅ Uploaded {count} events ({size} bytes) -> gs://{BUCKET}/{path}")

def commit_offsets():
    # commit current consumed offsets (safe after upload)
    consumer.commit()
    print("✅ Committed Kafka offsets")

print(f"Consumer starting: topic={TOPIC} bootstrap={BOOTSTRAP} group={GROUP_ID}")
print(f"GCS target: gs://{BUCKET}/{BASE_PREFIX}/dt=YYYY-MM-DD/hr=HH/")

try:
    for msg in consumer:
        # Convert to NDJSON line
        line = json.dumps(msg.value, separators=(",", ":"), ensure_ascii=False)
        buffer_lines.append(line)
        buffer_bytes += len(line) + 1

        # Flush conditions
        time_due = (time.time() - last_flush) >= FLUSH_SEC
        size_due = buffer_bytes >= MAX_BYTES
        count_due = len(buffer_lines) >= BATCH_SIZE

        if time_due or size_due or count_due:
            flush_to_gcs()
            commit_offsets()

except KeyboardInterrupt:
    print("Stopping... flushing remaining buffer.")

finally:
    # Final flush on exit
    if buffer_lines:
        flush_to_gcs()
        commit_offsets()
    consumer.close()
    print("Done.")
