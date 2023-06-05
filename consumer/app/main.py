from fastapi import FastAPI
import uvicorn
from kafka import KafkaConsumer, TopicPartition
import json
import os

app = FastAPI()

# load env variables
BROKER_SERVER = os.getenv("BROKER_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "foobar")


@app.get("/")
async def root():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BROKER_SERVER,
        # bootstrap_servers="kafka.default.svc.cluster.local",
        auto_offset_reset="latest",
        # consumer_timeout_ms=5000,
        value_deserializer=json.loads,
        enable_auto_commit=True,
        group_id=None,
    )
    # consumer.poll()
    # consumer.seek_to_end()
    # consumer.seek_to_end(TopicPartition(KAFKA_TOPIC, 0))
    return next(consumer).value


uvicorn.run(app, host="0.0.0.0", port=8000)
