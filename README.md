## Proje Dizi
cd /home/selcuk/bitirme/   

## Topic Oluşturma
./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-input-1 --partitions 3 --replication-factor 1

./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-activity --partitions 3 --replication-factor 1

./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-no-activity --partitions 3 --replication-factor 1

## Topic Listeleme
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Producer'ı Başlatma
cd /home/selcuk/scripts

bash producer.sh

## Spark Streaming'i Başlatma
python3 streaming.py


#  python3 streaming.py > producer.sh