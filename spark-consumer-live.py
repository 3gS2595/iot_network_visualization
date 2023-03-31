from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from geoip import geolite2

scala_version = '2.12'
spark_version = '3.1.2'
# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]

kafka_topic_name = "pkttest"
kafka_bootstrap_servers = "localhost:9092"

spark = SparkSession \
    .builder \
    .appName("Structured Streaming Pkt") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
pkt_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .load()

pkt_df1 = pkt_df.selectExpr("CAST(value AS STRING)", "timestamp")

pkt_schema_string = "Count STRING, src_mac STRING, dst_mac STRING"

pkt_df2 = pkt_df1 \
    .select(from_csv(col("value"), pkt_schema_string) \
            .alias("pkt"), "timestamp")
pkt_df3 = pkt_df2.select("pkt.*", "timestamp")

query = pkt_df3 \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
