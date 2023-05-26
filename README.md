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


<img src="https://user-images.githubusercontent.com/56341239/234556027-bd7108d2-2750-475e-b8f0-3f1c3e48baa0.png"  width="50%" height="20%">


