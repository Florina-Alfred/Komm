from kafka import KafkaConsumer, TopicPartition
import json
import os

# load env variables
BROKER_SERVER = os.getenv("BROKER_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "foobar")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BROKER_SERVER,
    # bootstrap_servers="kafka.default.svc.cluster.local",
    auto_offset_reset="latest",
    # consumer_timeout_ms=5000,
    value_deserializer=json.loads,
    group_id=None,
)

# consumer.assign(TopicPartition(topic="foobar", partition=0))
# consumer.seek_to_end()
# consumer.poll()
print("_" * 20)

# print(next(consumer), "_" * 20)
for msg in consumer:
    print(msg.value)

# print(next(consumer))
