import json
from kafka import KafkaConsumer


bootstrap_servers = ['localhost:9092']
topics = ['bitirme-activity', 'bitirme-no-activity']

consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
consumer.subscribe(topics)


while True:
    messages = consumer.poll(timeout_ms=1000)
    for topic_partition, message_list in messages.items():
        for message in message_list:
            if topic_partition.topic == "bitirme-activity":
                print("alert", f"{topic_partition.topic}",
                      message.value.decode('utf-8'))
