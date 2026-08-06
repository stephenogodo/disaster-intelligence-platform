from kafka import KafkaProducer
import json

print("Connecting...")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    api_version=(3, 9),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Connected!")

future = producer.send("smoke-test", {"message": "hello"})
metadata = future.get(timeout=30)

print(metadata)

producer.flush()
producer.close()

print("Done")