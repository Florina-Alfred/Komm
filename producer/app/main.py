from kafka import KafkaProducer
import uuid
import json
import time
import os
import cv2

# load env variables
BROKER_SERVER = os.getenv("BROKER_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "foobar")
KAFKA_COMPRESSION = os.getenv("KAFKA_COMPRESSION", "gzip")
KAFKA_COMPRESSION = os.getenv("KAFKA_COMPRESSION", "gzip")
CV2_ENCODE_SCALE = int(os.getenv("CV2_ENCODE_SCALE", "75"))
CV2_IMAGE_SCALE = int(os.getenv("CV2_IMAGE_SCALE", "3"))

producer = KafkaProducer(
    bootstrap_servers=[BROKER_SERVER],
    # bootstrap_servers=["kafka-stack-0.kafka-stack-headless.default.svc.cluster.local:9092"],
    # value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    compression_type=KAFKA_COMPRESSION,
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

            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), CV2_ENCODE_SCALE]
            ret, buffer = cv2.imencode(".jpg", frame[::CV2_IMAGE_SCALE,::CV2_IMAGE_SCALE], encode_param)
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
