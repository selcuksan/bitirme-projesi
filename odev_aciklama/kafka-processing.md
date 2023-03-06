# sudo systemctl start zookeeper
# sudo systemctl start kafka
# source ~/venvspark/bin/activate

# cd data-generator

# Create topic:
(venvspark) [train@10 data-generator]$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic office-input --replication-factor 1 --partitions 3

# Kafka producer: 
(venvspark) [train@10 data-generator]$ python dataframe_to_kafka.py --input /home/train/atscale4/final_homework/test_df/test_df.csv  -t office-input --excluded_cols 'pir_value' --sep ','

# Kafka consumer: 
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic office-input


# Let's create 2 topics for the streaming part
(venvspark) [train@10 data-generator]$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic office-activity --replication-factor 1 --partitions 3
(venvspark) [train@10 data-generator]$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic office-no-activity --replication-factor 1 --partitions 3


# Let's check if it's written on topics
kafka-console-consumer.sh --bootstrap-server localhost:9092 --c office-activity
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic office-no-activity
