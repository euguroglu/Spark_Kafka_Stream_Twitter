from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def write_to_cassandra(target_df, batch_id):
    target_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "tweet_db") \
        .option("table", "tweet2") \
        .mode("append") \
        .save()
    target_df.show()

if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    spark = SparkSession \
        .builder \
        .appName("PythonStreamingKafkaTweetCount") \
        .master("local[3]") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.cassandra.output.ignoreNulls", "true") \
        .getOrCreate()
    #Preparing schema for tweets
    schema = StructType([
        StructField("created_at", StringType()),
        StructField("id_str",IntegerType()),
    	StructField("text", StringType()),
        StructField("user", StructType([
            StructField("id",IntegerType()),
            StructField("name",StringType()),
            StructField("location",StringType())
        ])),
    ])
    #Read from kafka topic named "twitter"
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("startingOffsets", "earliest") \
        .load()

    #kafka_df.printSchema()

    value_df = kafka_df.select(from_json(col("value").cast("string"),schema).alias("value"))

    #value_df.printSchema()

    explode_df = value_df.selectExpr("value.created_at", "value.id_str", "value.text",
                                     "value.user.id", "value.user.name", "value.user.location")

    #explode_df.printSchema()
    #Converting created_at column to timestamp
    final_df = explode_df \
        .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))


    final_df.printSchema()


    output_query = final_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    output_query.awaitTermination()
    #kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING)")
    #tweets_table = kafka_df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")
    #user = tweets_table.select("user")
    #user_try = user.writeStream.format("console").option("truncate","false").start()
    #user_try.avaitTermination()
    #tweets = tweets_table.writeStream.format("console").option("truncate","false").start()
    #tweets_table.show()
    #tweets.awaitTermination()
    #query = kafka_df_string.writeStream.format("console").option("truncate","false").start()

    #query.awaitTermination()
