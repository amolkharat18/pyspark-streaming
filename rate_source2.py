from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("RateSourceDemo2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("rate") \
    .option("rowsPerSecond", 3).load()

final_df = df.withColumn("result", df.value % 3)

final_df.writeStream.format("console") \
    .outputMode("append").start() \
    .awaitTermination()
