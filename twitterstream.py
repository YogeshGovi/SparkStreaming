import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    kafka_topic_name = "TwiKaf"
    kafka_bootstrap_servers = 'master:9092'

    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("spark://master:7077") \
        .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0')\
        .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    #orders_df.show()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    # Write final result into console for debugging purpose
    orders_agg_write_stream = orders_df \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("append") \
        .option("truncate", "false") \
        .format("console") \
        .option("checkpointLocation", "/home/hduser/output") \
        .option("topic", "TwiKaf") \
        .start()
        # .format("kafka") \
        # .option("kafka.bootstrap.servers", "192.168.211.131:9093") \


    orders_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")




