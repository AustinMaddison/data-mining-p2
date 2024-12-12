from kafka import KafkaConsumer, TopicPartition
from elasticsearch import Elasticsearch
import json
from dotenv import load_dotenv

TOPIC_IN = "currency_data"
TOPIC_OUT = "financial-data"  # really this is an index for es, just incase we wanna chaing another process

load_dotenv()

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

es = Elasticsearch(["http://localhost:9200"])


def check_connection():
    try:
        if es.ping():
            print("Elasticsearch is running.")
        else:
            print("Elasticsearch connection failed.")
    except ConnectionError as e:
        print(f"Connection error: {e}")


def index_data(index_name, doc_id, doc):
    try:
        response = es.index(index=index_name, id=doc_id, body=doc)
        print(f"Document indexed successfully: {response}")
    except Exception as e:
        print(f"Error indexing document: {e}")


if __name__ == "__main__":
    check_connection()

    consumer = KafkaConsumer(TOPIC_IN, bootstrap_servers='localhost:29092'
                             , value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                             , auto_offset_reset='earliest', group_id=None)

    for message in consumer:
        data = message.value

        index_name = TOPIC_OUT
        doc_id = message.offset
        document = data

        index_data(index_name, doc_id, document)