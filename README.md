## Proje Dizinine Gitme

cd /home/selcuk/bitirme/kafka   

## Zookeeper ve Kafka server'ı çalıştırma

./bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 5
./bin/kafka-server-start.sh config/server.properties &

## Consumer Oluşturma
cd /home/selcuk/bitirme/kafka   

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitirme-input
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitirme-activity
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitirme-no-activity

# Producer'ı Başlatma

cd /home/selcuk/bitirme/simulate

python3 dataframe_to_kafka.py --input "/home/selcuk/bitirme/test_df/data.csv" -t bitirme-input --excluded_cols 'pir_value' --sep ',' --row_sleep_time=1

## Topic Oluşturma

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-input --partitions 3 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-activity --partitions 3 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-no-activity --partitions 3 --replication-factor 1

## Topic Listeleme

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Spark Streaming'i Başlatma

python3 streaming.py
