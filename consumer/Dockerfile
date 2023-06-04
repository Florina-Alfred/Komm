FROM python

WORKDIR /app

COPY requirements.txt requirements.txt
RUN apt update && apt install nano && pip install kafka-python
#RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "app/main.py"]
