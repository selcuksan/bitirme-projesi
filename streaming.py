import warnings

from alerts.activity_alert import ToAlert
from helpers.spark_helper import MyHelpers
from elastic.kafka_to_elastic import ToElastic
import multiprocessing

warnings.simplefilter(action='ignore')

my_helper_obj = MyHelpers()
to_elastic_obj = ToElastic()
to_alert_obj = ToAlert()

parallel_funcs = [to_elastic_obj.write_to_elastic, to_alert_obj.alert]
parallel_process = []
for func in parallel_funcs:
        process = multiprocessing.Process(target=func)
        process.start()
        parallel_process.append(process)

# get spark session
spark = my_helper_obj.get_spark_session()
spark.sparkContext.setLogLevel('ERROR')

data = my_helper_obj.get_data(
    spark_session=spark)


# # ML Processing
model_path = "/home/selcuk/bitirme/cv_model/pipeline_model"
transformed_df = my_helper_obj.get_transformed_df(model_path, data)

streamingQuery = transformed_df.writeStream \
    .foreachBatch(my_helper_obj.write_results) \
    .start().awaitTermination()


for process in parallel_process:
    process.join()
