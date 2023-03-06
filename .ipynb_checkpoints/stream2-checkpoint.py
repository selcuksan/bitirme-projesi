import warnings
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import PipelineModel
from utility.my_helpers import MyHelpers

my_helper_obj = MyHelpers()

warnings.simplefilter(action='ignore')
spark = (SparkSession.builder
         .appName("kafka_streaming")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

data = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "bitirme-input")
        .load())

data = data.selectExpr("CAST(value AS STRING)")

data = data.withColumn("co2_value", F.trim((F.split(F.col("value"), ",")[0])).cast("double")) \
    .withColumn("temp_value", F.trim((F.split(F.col("value"), ",")[1])).cast("double")) \
    .withColumn("light_value", F.trim((F.split(F.col("value"), ",")[2])).cast("double")) \
    .withColumn("humidity_value", F.trim((F.split(F.col("value"), ",")[3])).cast("double")) \
    .withColumn("time", F.trim((F.split(F.col("value"), ",")[4])).cast("timestamp")) \
    .withColumn("room", F.trim((F.split(F.col("value"), ",")[5]))) \
    .drop("value", "key", "topic", "partition", "offset", "timestamp")

data.printSchema()

model_path = "/home/selcuk/spark/bitirme-projesi/saved_model/pipeline_model"

loaded_pipeline_model = PipelineModel.load(model_path)
transformed_df = loaded_pipeline_model.transform(data)

transformed_df.printSchema()

streamingQuery = transformed_df.writeStream \
    .foreachBatch(my_helper_obj.write_results) \
    .start().awaitTermination()



