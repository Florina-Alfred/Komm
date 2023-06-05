from kafka import KafkaProducer
import uuid
import json
import time
import os
import cv2

# load env variables
BROKER_SERVER = os.getenv("BROKER_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "foobar")

producer = KafkaProducer(
    bootstrap_servers=[BROKER_SERVER],
    # bootstrap_servers=["kafka-0.kafka-headless.default.svc.cluster.local:9092"],
    # value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

vid = cv2.VideoCapture(2)
while True:
    ret, frame = vid.read()
    cv2.imshow("frame", frame)

    ret, buffer = cv2.imencode(".jpg", frame)
    producer.send("webcam", buffer.tobytes())

    if cv2.waitKey(1) & 0xFF == ord("q"):
        break

vid.release()
cv2.destroyAllWindows()
producer.flush()
