FROM python:3.13-slim

WORKDIR /app

COPY docker/producer-requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/streamflow ./src/streamflow
COPY config ./config

WORKDIR /app/src/streamflow

CMD ["python", "producer.py"]
