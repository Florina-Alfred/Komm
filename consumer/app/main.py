from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import uvicorn
from kafka import KafkaConsumer, TopicPartition
import json
import os
import cv2
import numpy as np
import time
from prometheus_client import make_asgi_app

app = FastAPI()

# load env variables
BROKER_SERVER = os.getenv("BROKER_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "webcam")


def gen_frame(local_kafka_topic):
    consumer = KafkaConsumer(
        local_kafka_topic,
        bootstrap_servers=BROKER_SERVER,
        # bootstrap_servers="kafka-stack.default.svc.cluster.local",
        auto_offset_reset="latest",
        # consumer_timeout_ms=5000,
        # value_deserializer=json.loads,
        enable_auto_commit=False,
        # group_id=None,
    )
    # consumer.poll()
    # consumer.seek_to_end()
    # consumer.seek_to_end(TopicPartition(KAFKA_TOPIC, 0))

    now = time.time()
    for msg in consumer:
        if time.time() - now >= 5:
            now = time.time()
            print(now)
        nparr = np.frombuffer(msg.value, np.uint8)
        frame = nparr.tobytes()

        yield (b'--frame\r\n' b'Content-Type: image\r\n\r\n' + frame + b'\r\n')


@app.get("/")
async def root():
    return StreamingResponse(
        gen_frame(KAFKA_TOPIC), media_type='multipart/x-mixed-replace; boundary=frame'
    )

@app.get("/healthy")
async def root():
    return {"healthy":True, "time": time.time()}


@app.get("/frame/{dynamic_frame}")
async def root(dynamic_frame):
    return StreamingResponse(
        gen_frame(dynamic_frame), media_type='multipart/x-mixed-replace; boundary=frame'
    )

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

if __name__ == "__main__":
    print(f"Starting a K-Con on {KAFKA_TOPIC} @ {BROKER_SERVER}")
    uvicorn.run(app, host="0.0.0.0", port=8000)
