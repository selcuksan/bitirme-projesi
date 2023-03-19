## Source of Dataset : 
- https://www.kaggle.com/ranakrc/smart-building-system

## About Dataset :
This dataset is collected from 255 sensor time series, instrumented in 51 rooms in 4 floors of the Sutardja Dai Hall(SDH) at UC Berkeley. It can be used to investigate patterns in physical properties of a room in a building. Moreover, it can also be used for experiments relating to Internet-of-Things (IoT), sensor fusion network or time-series tasks. This dataset is suitable for both supervised (classification and regression) and unsupervised learning (clustering) tasks.

Each room includes 5 types of measurements: 
- CO2 concentration, 
- humidity: room air humidity, 
- temperature: room temperature,
- light: luminosity, 
- PIR: PIR motion sensor data

Data was collected over a period of one week from Friday, August 23, 2013 to Saturday, August 31, 2013. The PIR motion sensor is sampled once every 10 seconds and the remaining sensors are sampled once every 5 seconds. Each file contains the timestamps and actual readings from the sensor.

The passive infrared sensor (PIR sensor) is an electronic sensor that measures infrared (IR) light radiating from objects in its field of view, which measures the occupancy in a room. Approximately 6% of the PIR data is non-zero, indicating an occupied status of the room. The remaining 94% of the PIR data is zero, indicating an empty room.

## Tasks:
1. Using this data set, develop a machine learning model that predicts whether there is any activity in a room with known CO2, humidity, temperature, light and time information.

2. Download the `test` dataset that you used while developing the ML model. Then generate this set with the data-generator into a topic named Kafka `office-input` excluding the target variable (pir).

3. Consume Kafka `office-input` topic with Spark streaming. Estimate the activity information using your model (activity, no-activity in the room).

4. If there is activity in the room, produce it in the `office-activity` topic, and if there is no activity in the `office-no-activity` topic, including the room number and time information.
