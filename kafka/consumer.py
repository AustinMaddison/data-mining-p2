from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

consumer = KafkaConsumer('financial-data', bootstrap_servers='localhost:29092', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
es = Elasticsearch(['http://localhost:9200'])

for message in consumer:
    data = message.value
    es.index(index='financial-data', body=data)