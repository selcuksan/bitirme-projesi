## Proje Dizi
cd /home/selcuk/bitirme/   

# Producer'ı Başlatma
cd /home/selcuk/bitirme/simulate

python3 dataframe_to_kafka.py --input "/home/selcuk/bitirme/test_df/data.csv" -t bitirme-input --excluded_cols 'pir_value' --sep ',' --row_sleep_time=1

## Topic Oluşturma
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-input-1 --partitions 3 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-activity --partitions 3 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bitirme-no-activity --partitions 3 --replication-factor 1

## Topic Listeleme
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

## Spark Streaming'i Başlatma
python3 streaming.py


# producer.sh > python3 streaming.py