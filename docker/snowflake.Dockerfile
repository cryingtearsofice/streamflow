FROM python:3.11-slim

WORKDIR /app

COPY docker/snowflake-requirements.txt /tmp/snowflake-requirements.txt
RUN pip install --no-cache-dir -r /tmp/snowflake-requirements.txt
