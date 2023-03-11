import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch


es = Elasticsearch("https://kafka-test.es.us-central1.gcp.cloud.es.io:9243",
                   basic_auth=["elastic", "NsaZVRzNUC6kRyCgLbVhePaF"], verify_certs=False)


consumer = KafkaConsumer(
    'office-input2',
    bootstrap_servers=['localhost : 9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='group')

for num, msg in enumerate(consumer):
    message = msg.value
    string = message.decode("ascii").split(",")
    json_string = {
        "co2_value": float(string[0]),
        "temp_value": float(string[1]),
        "light_value": float(string[2]),
        "humidity_value": float(string[3]),
        "time": string[4],
        "room": str(string[5]),
        "label": str(string[6])
    }

    json_string = json.dumps(json_string)
    # print(message, json_string)
    resp = es.index(index="bitirme2", id=num, document=json_string)
    # print(resp["result"])
    # break

# b'508.0,23.79,78.0,55.05,2013-08-28T04:53:33.000+03:00,415,0' {"co2_value": 508.0, "temp_value": 23.79, "light_value": 78.0, "humidity_value": 55.05, "time": "2013-08-28T04:53:33.000+03:00", "room": "415", "label": "0"}
# b'508.0,23.79,78.0,55.05,2013-08-28T04:53:33.000+03:00,415,0' {"co2_value": 508.0, "temp_value": 23.79, "light_value": 78.0, "humidity_value": 55.05, "time": "2013-08-28T04:53:33.000+03:00", "room": "415", "label": "0"}
