cd /home/selcuk/bitirme/kafka   
./bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 10
echo "zookeeper calisti"
./bin/kafka-server-start.sh config/server.properties &
sleep 2
echo "kafka calisti"