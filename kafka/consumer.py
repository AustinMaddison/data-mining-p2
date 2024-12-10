from kafka import KafkaConsumer, TopicPartition
from elasticsearch import Elasticsearch
import json

import os
from dotenv import load_dotenv

load_dotenv()
financial_data = os.getenv('STOCK_LOG')
consumer = KafkaConsumer('financial-data', bootstrap_servers='localhost:29092'
                         , value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                         , auto_offset_reset='earliest', group_id=None)
es = Elasticsearch(['http://localhost:9200'])

partition = TopicPartition('financial-data', 0)
end_offset = consumer.end_offsets([partition])
consumer.seek(partition, list(end_offset.values())[0]-1)
for message in consumer:
    data = message.value
    print(data)
    es.index(index='financial-data', body=data)

