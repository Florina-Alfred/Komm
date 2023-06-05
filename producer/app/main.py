from kafka import KafkaProducer
import uuid
import json
import time

producer = KafkaProducer(
    bootstrap_servers=["kafka-0.kafka-headless.default.svc.cluster.local:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


while True:
    v = str(uuid.uuid4())
    print(v)
    producer.send("foobar", {"id": v, "time": str(time.time())})
    time.sleep(1)
producer.flush()
