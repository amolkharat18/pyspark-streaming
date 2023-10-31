from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a SparkContext
sc = SparkContext(appName="DStreamExample")
# Create a StreamingContext with a batch interval of 10 second
ssc = StreamingContext(sparkContext=sc, batchDuration=10)

sc.setLogLevel('ERROR')

# Define the HDFS directory path
hdfs_directory = "path"

# Create a DStream that monitors the HDFS directory for new files
SensorDataStream = ssc.textFileStream(hdfs_directory)

# Apply Filter operation based on VibrationSignal field
FilteredDStream = SensorDataStream.filter(lambda record: int(record.split(",")[5]) == 1)

# Apply map operation and select required columns
mapDStream = FilteredDStream.map(lambda record: (record.split(",")[0],
                                                 record.split(",")[1],
                                                 record.split(",")[2],
                                                 int(record.split(",")[5])))
mapDStream.pprint()

# Start the streaming context
ssc.start()
# Wait for the streaming context to terminate
ssc.awaitTermination()
