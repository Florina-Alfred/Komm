from kafka import KafkaConsumer, TopicPartition
import json

consumer = KafkaConsumer(
    "foobar",
    bootstrap_servers="kafka.default.svc.cluster.local",
    auto_offset_reset="latest",
    # consumer_timeout_ms=5000,
    value_deserializer=json.loads,
    group_id=None,
)

# consumer.assign(TopicPartition(topic="foobar", partition=0))
# consumer.seek_to_end()
# consumer.poll()
print("_" * 20)

print(next(consumer), "_" * 20)
for msg in consumer:
    print(msg.value, json(msg))

# print(next(consumer))
