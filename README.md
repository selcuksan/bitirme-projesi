# Çalışmayı başlatmak için:
Spark konteynerine erişmek için aşağıdaki komutu kullanın:
```shell
docker exec -it spark-container bash
```
Spark konteyneri içerisindeki Proje dizinine gidin
```shell
cd /home/selcuk/bitirme/
```

`streaming.py` dosyasını çalıştırın:  
```shell
python3 streaming.py
```

Ardından, yeni bir terminal açın ve scripts dizinine gidin:
```shell
cd /home/selcuk/bitirme/scripts
```

producer.sh scriptini çalıştırın:
```shell
bash producer.sh
```

## Spark
Spark konteynerine tarayıcınızdan http://localhost:8888/ adresini kullanarak erişebilirsiniz.

## Elasticsearch
Elasticsearch'e tarayıcınızdan http://localhost:9200/ adresini kullanarak erişebilirsiniz.

## Kibana
Kibana'ya tarayıcınızdan http://localhost:5601/ adresini kullanarak erişebilirsiniz.

## Kafka
Kafka konteynerine erişmek için aşağıdaki komutu kullanın:
```shell
docker exec -it kafka-container bash
```

##### Topic Listeleme
Kafka'da mevcut olan topic'leri listelemek için aşağıdaki komutu kullanabilirsiniz:

    ./bin/kafka-topics --bootstrap-server localhost:9092 --list

##### Topic Oluşturma
Yeni bir Kafka topic oluşturmak için aşağıdaki komutları kullanabilirsiniz:

```shell
./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-input-1 --partitions 3 --replication-factor 1
```

```shell
./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-activity --partitions 3 --replication-factor 1
```

```shell
./bin/kafka-topics --bootstrap-server localhost:9092 --create --topic bitirme-no-activity --partitions 3 --replication-factor 1
```


<<<<<<< HEAD
<img src="https://user-images.githubusercontent.com/56341239/234556027-bd7108d2-2750-475e-b8f0-3f1c3e48baa0.png"  width="50%" height="20%">
=======
<img src="https://user-images.githubusercontent.com/56341239/234556027-bd7108d2-2750-475e-b8f0-3f1c3e48baa0.png"  width="50%" height="20%">


>>>>>>> 55e61960bf18d9afcc909a2db6884237153c91c7
