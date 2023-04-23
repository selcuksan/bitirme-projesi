import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from config import KAFKA, ELASTIC
from datetime import datetime, timezone, timedelta


def create_consumer():
    consumer = KafkaConsumer(
        KAFKA["TOPIC_INPUT"],
        bootstrap_servers=[KAFKA["SERVER"]],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA["TOPIC_INPUT"])
    return consumer


class ToElastic(object):
    def __init__(self):
        self.es = Elasticsearch(ELASTIC["SERVER"])
        self.consumer = create_consumer()

    def write_to_elastic(self):
        for msg in self.consumer:
            message = msg.value
            timestamp = msg.timestamp / 1000  
            date = datetime.fromtimestamp(timestamp, timezone(
                timedelta(hours=3))).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
            string = message.decode("ascii").split(",")
            json_string = {
                "co2_value": float(string[0]),
                "temp_value": float(string[1]),
                "light_value": float(string[2]),
                "humidity_value": float(string[3]),
                "time": date,
                "room": str(string[4])
            }
            json_string = json.dumps(json_string)
            resp = self.es.index(
                index=ELASTIC["INDEX_INPUT"], body=json_string)
            # print(resp)
