from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import uvicorn
from kafka import KafkaConsumer, TopicPartition
import json
import os
import cv2
import numpy as np

app = FastAPI()

# load env variables
BROKER_SERVER = os.getenv("BROKER_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "webcam")


def gen_frame():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BROKER_SERVER,
        # bootstrap_servers="kafka.default.svc.cluster.local",
        auto_offset_reset="latest",
        # consumer_timeout_ms=5000,
        # value_deserializer=json.loads,
        enable_auto_commit=False,
        group_id=None,
    )
    # consumer.poll()
    # consumer.seek_to_end()
    # consumer.seek_to_end(TopicPartition(KAFKA_TOPIC, 0))

    for msg in consumer:
        nparr = np.frombuffer(msg.value, np.uint8)
        frame = nparr.tobytes()
        yield (b"--frame\r\n" b"Content-Type: image\r\n\r\n" + frame + b"\r\n")


@app.get("/")
async def root():
    return StreamingResponse(
        gen_frame(), media_type="multipart/x-mixed-replace; boundary=frame"
    )


uvicorn.run(app, host="0.0.0.0", port=8000)
