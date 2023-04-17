import json
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import requests


def create_consumer():
    bootstrap_servers = ['localhost:9092']
    topics = ['bitirme-activity', 'bitirme-no-activity']

    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    consumer.subscribe(topics)
    return consumer


class ToAlert(object):
    es = Elasticsearch("http://elastic-container:9200")
    consumer = create_consumer()

    def __init__(self) -> None:
        pass

    def alert(self):
        while True:
            messages = ToAlert.consumer.poll()  # timeout_ms=1000
            for topic_partition, message_list in messages.items():
                for message in message_list:
                    string = str(message.value.decode('utf-8')).split("---")
                    string = {
                        'date': string[0].strip(),
                        'room': string[1].strip(),
                        'alert-type': string[2].strip()
                    }
                    json_string = json.dumps(string)
                    ToAlert.es.index(
                        index="bitirme-alert-1", body=json_string)
                    if topic_partition.topic == "bitirme-activity":
                        print(json_string)
                        send_slack_message(string)
                    elif topic_partition.topic == "bitirme-no-activity":
                        continue
                    else:
                        print("Hata")


def send_slack_message(payload):
    webhook_url = 'https://hooks.slack.com/services/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'
    return requests.post(url=webhook_url, json= {"text": str(payload)}, headers={'Content-Type': 'application/json'})
