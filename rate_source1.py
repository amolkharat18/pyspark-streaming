from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("RateSourceDemo1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("rate") \
    .option("rowsPerSecond", 3).load()

df.writeStream.format("console") \
    .outputMode("append").start() \
    .awaitTermination()
