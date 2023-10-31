from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, ShortType, IntegerType, DoubleType

# Create a SparkSession
spark = SparkSession.builder\
    .appName("Streaming-Join-Demo")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

sensor_schema = StructType()\
    .add("SensorID", StringType(), True)\
    .add("MachineID", StringType(), True)\
    .add("Temperature", DoubleType(), True)\
    .add("Pressure", IntegerType(), True)\
    .add("Speed", IntegerType(), True)\
    .add("VibrationSignal", ShortType(), True)

machine_schema = StructType()\
    .add("MachineID", StringType(), True)\
    .add("MachineType", StringType(), True)\
    .add("Age", IntegerType(), True)

# Define the HDFS directory path to monitor
input_directory = "join_demo/"

# Create a streaming DataFrame by reading from the HDFS directory
streaming_df = spark.readStream\
    .option("header", "True")\
    .schema(sensor_schema)\
    .csv(input_directory)

# Create a static DataFrame by reading from the HDFS directory
file_path = "data/machines.csv"
static_df = spark.read\
    .schema(machine_schema)\
    .csv(file_path)

agg_df = streaming_df\
    .where("VibrationSignal = 1")\
    .groupby("MachineID")\
    .count()\
    .where("count > 3")

join_df = agg_df.join(static_df, 'MachineID').where('age > 5')

query = join_df.writeStream\
    .outputMode("complete")\
    .format("console")\
    .start()
query.awaitTermination()
