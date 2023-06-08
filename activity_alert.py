import json
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import requests
from config import KAFKA, ELASTIC, SLACK_URL
from datetime import datetime
from datetime import datetime, timezone, timedelta


def create_consumer():
    bootstrap_servers = [KAFKA["SERVER"]]
    topics = [KAFKA["TOPIC_NO_ACTIVITY"], KAFKA["TOPIC_ACTIVITY"]]

    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id="TOPIC_ALERT")
    consumer.subscribe(topics)
    return consumer


class ToAlert(object):
    def __init__(self) -> None:
        self.es = Elasticsearch(ELASTIC["SERVER"])
        self.consumer = create_consumer()

    def alert(self):
        while True:
            messages = self.consumer.poll()
            for topic_partition, message_list in messages.items():
                for message in message_list:
                    timestamp = message.timestamp / 1000
                    date = datetime.fromtimestamp(timestamp, timezone(
                        timedelta(hours=3))).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
                    string = str(message.value.decode('utf-8')).split("---")
                    string = {
                        'date': date,
                        'room': string[1].strip(),
                        'alert-type': string[2].strip()
                    }
                    json_string = json.dumps(string)
                    self.es.index(
                        index=ELASTIC["INDEX_ALERT"], body=json_string)
                    if topic_partition.topic == KAFKA["TOPIC_ACTIVITY"]:
                        send_slack_message(json_string)
                    elif topic_partition.topic == KAFKA["TOPIC_NO_ACTIVITY"]:
                        continue
                    else:
                        print("Hata: TOPIC Bulunamadı")


def send_slack_message(payload):
    webhook_url = SLACK_URL
    response = requests.post(url=webhook_url, json={"text": str(payload)}, headers={
        'Content-Type': 'application/json'})
    print(response)
    return response
