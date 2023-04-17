import json
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import requests
from config import KAFKA, ELASTIC


def create_consumer():
    bootstrap_servers = [KAFKA["SERVER"]]
    topics = [KAFKA["TOPIC_NO_ACTIVITY"], KAFKA["TOPIC_ACTIVITY"]]

    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, auto_offset_reset='latest',
                             enable_auto_commit=True, group_id="TOPIC_ALERT")
    consumer.subscribe(topics)
    return consumer


class ToAlert(object):
    es=Elasticsearch(ELASTIC["SERVER"])
    consumer=create_consumer()

    def __init__(self) -> None:
        pass

    def alert(self):
        while True:
            messages=ToAlert.consumer.poll()  # timeout_ms=1000
            for topic_partition, message_list in messages.items():
                for message in message_list:
                    string=str(message.value.decode('utf-8')).split("---")
                    string={
                        'date': string[0].strip(),
                        'room': string[1].strip(),
                        'alert-type': string[2].strip()
                    }
                    json_string = json.dumps(string)
                    ToAlert.es.index(
                        index=ELASTIC["INDEX_ALERT"], body=json_string)
                    if topic_partition.topic == KAFKA["TOPIC_ACTIVITY"]:
                        print(json_string)
                        send_slack_message(string)
                    elif topic_partition.topic == KAFKA["TOPIC_NO_ACTIVITY"]:
                        continue
                    else:
                        print("Hata")


def send_slack_message(payload):
    webhook_url = 'https://hooks.slack.com/services/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'
    return requests.post(url=webhook_url, json={"text": str(payload)}, headers={'Content-Type': 'application/json'})
