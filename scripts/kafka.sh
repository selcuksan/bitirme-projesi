cd /home/selcuk/bitirme/kafka   

./bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 20 &
./bin/kafka-server-start.sh config/server.properties


