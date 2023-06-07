FROM python:3.9.13-slim-buster

WORKDIR /app
RUN DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y && apt install python3-pip ffmpeg libsm6 libxext6 -y
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "app/main.py"]
