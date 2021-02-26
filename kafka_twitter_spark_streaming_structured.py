"""
RUNNING PROGRAM;

1-Start Apache Kafka
./kafka/kafka_2.11-0.11.0.0/bin/kafka-server-start.sh ./kafka/kafka_2.11-0.11.0.0/config/server.properties

2-Run kafka_push_listener.py (Start Producer)
ipython >> run kafka_push_listener.py

3-Run kafka_twitter_spark_streaming.py (Start Consumer)
PYSPARK_PYTHON=python3 bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 ~/Documents/kafka_twitter_spark_streaming.py

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

schema = StructType([
    StructField("user", StringType(), True),
	StructField("text", StringType(), True)
])

if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    spark = SparkSession.builder.appName("PythonStreamingKafkaTweetCount").getOrCreate()

    kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").option("startingOffsets", "earliest").load()
    kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING)")
    tweets_table = kafka_df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")
    #user = tweets_table.select("user")
    #user_try = user.writeStream.format("console").option("truncate","false").start()
    #user_try.avaitTermination()
    tweets = tweets_table.writeStream.format("console").option("truncate","false").start()
    tweets_table.show()
    tweets.awaitTermination()
    #query = kafka_df_string.writeStream.format("console").option("truncate","false").start()

    #query.awaitTermination()
