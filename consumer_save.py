from kafka import KafkaConsumer
import json
import boto3
from datetime import datetime

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

s3 = boto3.client("s3")

bucket_name = "sneha-data-engineering-project-2026"

print("Listening to Kafka...")

for message in consumer:
    data = message.value

    file_name = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    local_file = f"/tmp/{file_name}"

    with open(local_file, "w") as f:
        json.dump(data, f)

    s3.upload_file(
        local_file,
        bucket_name,
        f"bronze/orders/{file_name}"
    )

    print(f"Uploaded {file_name} to S3")