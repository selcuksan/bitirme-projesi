import json
from time import sleep
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from config import KAFKA, ELASTIC


def create_consumer():
    consumer = KafkaConsumer(
        KAFKA["TOPIC_INPUT"],
        bootstrap_servers=[KAFKA["SERVER"]],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA["TOPIC_INPUT"])
    return consumer


class ToElastic(object):
    consumer = create_consumer()
    es = Elasticsearch(ELASTIC["SERVER"])

    def __init__(self):
        pass

    def write_to_elastic(self):
        # sleep(1)
        for num, msg in enumerate(ToElastic.consumer):
            message = msg.value
            string = message.decode("ascii").split(",")
            json_string = {
                "co2_value": float(string[0]),
                "temp_value": float(string[1]),
                "light_value": float(string[2]),
                "humidity_value": float(string[3]),
                "time": str(string[4]),
                "room": str(string[5])
                # , "label": str(string[6])
            }

            json_string = json.dumps(json_string)
            # print(json_string)
            resp = ToElastic.es.index(
                index=ELASTIC["INDEX_INPUT"], body=json_string)
            # print(resp)
