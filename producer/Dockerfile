FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN apt update && apt install ffmpeg libsm6 libxext6 libcanberra-gtk-module libcanberra-gtk3-module -y
RUN pip3 install -r requirements.txt
COPY . .

CMD [ "python3", "app/main.py"]
