from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

consumer = KafkaConsumer('financial-data', bootstrap_servers='localhost:29092', value_deserializer=lambda m: json.loads(m.decode('utf-8')), auto_offset_reset='earliest', group_id=None)
es = Elasticsearch(['http://localhost:9200'])

for message in consumer:
    data = message.value
    print(data['Meta Data'])
    es.index(index='financial-data', body=data['Meta Data'])