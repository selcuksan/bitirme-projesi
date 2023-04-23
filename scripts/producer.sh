cd /home/selcuk/bitirme

python3 dataframe_to_kafka.py --input "/home/selcuk/bitirme/test_df/data-simulation.csv" -t bitirme-input-1 --excluded_cols 'pir_value' --sep ',' --row_sleep_time=1

# test-data-sampled.csv
# data-simulation.csv