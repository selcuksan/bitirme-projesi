## Proje Dizinine Gitme
cd /home/selcuk/bitirme/kafka


## Zookeeper ve Kafka server'ı çalıştırma
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties

## Consumer Oluşturma
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitirme-input
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitirme-activity
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitirme-no-activity


## Topic Oluşturma
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-input --partitions 3 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-activity --partitions 3 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-no-activity --partitions 3 --replication-factor 1

## Topic Listeleme
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

python3 dataframe_to_kafka.py --input "/home/selcuk/bitirme/test_df/data.csv" -t bitirme-input --excluded_cols 'pir_value' --sep ','

python3 streaming.py


elastic password: DdU+*3WfBd20Iuo-XD+T

