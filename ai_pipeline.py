# pip install tensorflow
# pip install transformers
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from transformers import pipeline

# Create a SparkContext
sc = SparkContext(appName="SentimentAnalysis")

# Create a StreamingContext with a batch interval of 10 second
ssc = StreamingContext(sparkContext=sc, batchDuration=10)

# Create a stream that simulates incoming text data (replace this with your data source)
input_stream = ssc.textFileStream('ai_pipeline_demo/')

# Initialize a sentiment analysis pipeline using the Transformers library
nlp = pipeline("sentiment-analysis")


# Define a function to perform sentiment analysis on incoming text
def perform_sentiment_analysis(rdd):
    records = rdd.collect()
    print('records:', records)

    for data in records:
        result = nlp(data)
        sentiment = result[0]['label']
        score = result[0]['score']
        print(f"Text: {data}")
        print(f"Sentiment: {sentiment}")
        print(f"Confidence Score: {score}")
        print("")


# Apply the sentiment analysis function to the input stream
input_stream.foreachRDD(perform_sentiment_analysis)

# Start the streaming context
ssc.start()

# Terminate the streaming context
ssc.awaitTermination()
