FROM python:3.9.13-slim-buster

WORKDIR /app
RUN DEBIAN_FRONTEND=noninteractive

COPY requirements.txt requirements.txt
RUN apt update && apt install python3-pip ffmpeg libsm6 libxext6 libcanberra-gtk-module libcanberra-gtk3-module -y
RUN pip3 install -r requirements.txt
COPY . .

CMD [ "python3", "app/main.py"]
