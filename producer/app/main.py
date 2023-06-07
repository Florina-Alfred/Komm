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
    # bootstrap_servers=["kafka-stack-0.kafka-stack-headless.default.svc.cluster.local:9092"],
    # value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Starting a K-Pro on {KAFKA_TOPIC} @ {BROKER_SERVER}")

for i in range(10):
    print(f"Testing camera {i} to start K-Pro with")
    try:
        vid = cv2.VideoCapture(i)
        print(f"Choose the camera {i}")
        now = time.time()
        while True:
            ret, frame = vid.read()
            # cv2.imshow("frame", frame)

            ret, buffer = cv2.imencode(".jpg", frame[::2,::2,0])
            producer.send(KAFKA_TOPIC, buffer.tobytes())
            if time.time() - now >= 5:
                now = time.time()
                print(now)

            #if cv2.waitKey(1) & 0xFF == ord("q"):
            #    break
    except:
        continue

vid.release()
cv2.destroyAllWindows()
producer.flush()
