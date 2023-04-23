## Proje Dizi

    cd /home/selcuk/bitirme/   
    python3 streaming.py 
    bash scripts/producer.sh


### Spark Streaming
    python3 streaming.py

### Producer
    cd /home/selcuk/scripts

    bash producer.sh

### Topic Listeleme
    ./bin/kafka-topics --bootstrap-server localhost:9092 --list

### Topic Oluşturma
    ./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-input-1 --partitions 3 --replication-factor 1

    ./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-activity --partitions 3 --replication-factor 1

    ./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-no-activity --partitions 3 --replication-factor 1

