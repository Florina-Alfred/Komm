from kafka import KafkaProducer
import uuid
import json
import time
import os

# load env variables
BROKER_SERVER = os.getenv("BROKER_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "foobar")

producer = KafkaProducer(
    bootstrap_servers=[BROKER_SERVER],
    # bootstrap_servers=["kafka-0.kafka-headless.default.svc.cluster.local:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


while True:
    v = str(uuid.uuid4())
    print(v)
    producer.send(KAFKA_TOPIC, {"id": v, "time": str(time.time())})
    time.sleep(1)
producer.flush()
