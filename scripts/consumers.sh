cd /home/selcuk/bitirme/kafka   
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitirme-activity &
sleep 5
echo "consumer bitirme-activity"
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitirme-no-activity &
sleep 5
echo "consumer bitirme-activity"