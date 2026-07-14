# streamflow

## Python to JSON
Install Pydantic to make sure that you can convert your Python classes to JSON:
`pip install pydantic`

## Kafka 
Download [Kafka 4.3.1](https://kafka.apache.org/community/downloads/)

Run these three commands in order:
`kafka-storage.bat random-uuid`,
`kafka-storage.bat format --standalone -t <uuid> -c config\server.properties`,
`kafka-server-start.bat config\server.properties`

## Daily Stand up Notes
[Google Doc Link](https://docs.google.com/document/d/1vBSWKS6I9iNOImxK-9I53IPVQxgbxC305xiBsHl-q8I/edit?usp=sharing)