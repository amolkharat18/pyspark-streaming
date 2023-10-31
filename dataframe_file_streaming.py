from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, ShortType, IntegerType, DoubleType

# Create a SparkSession
spark = SparkSession.builder\
    .appName("File-Streaming-Demo")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

sensor_schema = StructType()\
    .add("SensorID", StringType(), True)\
    .add("MachineID", StringType(), True)\
    .add("Temperature", DoubleType(), True)\
    .add("Pressure", IntegerType(), True)\
    .add("Speed", IntegerType(), True)\
    .add("VibrationSignal", ShortType(), True)

# Define the HDFS directory path to monitor
hdfs_directory = "hdfs://chnntxetarh14:9000/user/amol_trng/pyspark_streaming/hdfs_demo"

# Create a streaming DataFrame by reading from the HDFS directory
streaming_df = spark.readStream\
    .option("header", "True")\
    .schema(sensor_schema)\
    .csv(hdfs_directory)

print("isStreaming:", streaming_df.isStreaming)

query = streaming_df.writeStream\
    .format("console")\
    .start()
query.awaitTermination()
