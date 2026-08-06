FROM apache/spark:3.5.0

USER root
COPY docker/spark-requirements.txt /tmp/spark-requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/spark-requirements.txt
USER spark
