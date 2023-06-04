FROM python:3.12.0b1-alpine3.18

WORKDIR /app

COPY requirements.txt requirements.txt
RUN apt update && apt install ffmpeg libsm6 libxext6 -y && pip install kafka-python
#RUN pip3 install -r requirements.txt
COPY . .

CMD [ "python3", "app/main.py"]
