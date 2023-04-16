import warnings
from helpers.spark_helper import MyHelpers
from pyspark.sql import SparkSession, functions as F
from elastic.kafka_to_elastic import ToElastic
import multiprocessing


warnings.simplefilter(action='ignore')

my_helper_obj = MyHelpers()
to_elastic_obj = ToElastic()

process = multiprocessing.Process(target=to_elastic_obj.write_to_elastic)
process.start()

# get spark session
spark = my_helper_obj.get_spark_session()

spark.sparkContext.setLogLevel('ERROR')

data = my_helper_obj.get_data(
    spark_session=spark)

# data.printSchema()
# # ML Processing

model_path = "/home/selcuk/bitirme/cv_model/pipeline_model"
transformed_df = my_helper_obj.get_transformed_df(model_path, data)

transformed_df.printSchema()

streamingQuery = transformed_df.writeStream \
    .foreachBatch(my_helper_obj.write_results) \
    .start().awaitTermination()

# streamingQuery.awaitTermination()

process.join()