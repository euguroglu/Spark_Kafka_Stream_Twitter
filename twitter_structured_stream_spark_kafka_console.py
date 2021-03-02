from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    spark = SparkSession \
        .builder \
        .appName("PythonStreamingKafkaTweetCount") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
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

    console_query = final_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "chk-point-dir") \
    .trigger(processingTime="1 minute") \
    .start()

    console_query.awaitTermination()
