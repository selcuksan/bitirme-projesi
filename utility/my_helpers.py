from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import PipelineModel


class MyHelpers:
    def get_spark_session(self, session_params: dict) -> SparkSession:
        # Put your code here.
        spark = (SparkSession.builder
                 .appName("kafka_streaming")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
                 .getOrCreate())
        return spark

    def get_data(self, spark_session: SparkSession) -> DataFrame:
        # Put your code here
        data = (spark_session.readStream
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
        return data

    def get_transformed_df(self, model_path, data):
        loaded_pipeline_model = PipelineModel.load(model_path)
        transformed_df = loaded_pipeline_model.transform(data)
        return transformed_df

    def write_results(self, df, batchId):
        # df.cache()
        # df.show(5)
        # df_columns = df.columns
        office_activitiy = df.filter("prediction == 1")
        office_activitiy.show(1)
        office_activitiy.withColumn("value", F.concat(F.col("time"), F.lit('---'), F.col("room"), F.lit('---'), F.col("prediction"))).selectExpr("CAST(value AS STRING)").write.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "bitirme-activity") \
            .save()

        office_no_activity = df.filter("prediction == 0")
        office_no_activity.show(1)
        office_no_activity.withColumn("value", F.concat(F.col("time"), F.lit('---'), F.col("room"), F.lit('---'), F.col("prediction"))).selectExpr("CAST(value AS STRING)").write.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "bitirme-no-activity") \
            .save()
