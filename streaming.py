import warnings
from utility.my_helpers import MyHelpers
from pyspark.sql import SparkSession, functions as F

warnings.simplefilter(action='ignore')

my_helper_obj = MyHelpers()

# get spark session
session_params_dict = {
    "appName": "kafka_streaming",
    "config": ("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
}

spark = my_helper_obj.get_spark_session(session_params_dict)

spark.sparkContext.setLogLevel('ERROR')


data = my_helper_obj.get_data(
    spark_session=spark)

data.printSchema()
# # ML Processing

model_path="/home/selcuk/spark/bitirme-projesi/saved_model/pipeline_model"
transformed_df = my_helper_obj.get_transformed_df(model_path, data)

transformed_df.printSchema()

streamingQuery = transformed_df.writeStream \
    .foreachBatch(my_helper_obj.write_results) \
    .start().awaitTermination()

# streamingQuery.awaitTermination()
