from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col 
from pyspark.sql.types import StructType, StringType

# Define the schema for your JSON data
schema = StructType() \
    .add("artist", StringType()) \
    .add("track", StringType()) \
    .add("url", StringType())

# Start Spark session
spark = SparkSession.builder \
    .appName("KafkaSpotifyJSON") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
            "org.apache.kafka:kafka-clients:3.9.0,"
            "org.lz4:lz4-java:1.8.0,"
            "org.xerial.snappy:snappy-java:1.1.10.5,"
            "org.apache.commons:commons-pool2:2.11.1") \
    .getOrCreate()

# Set logging level
spark.sparkContext.setLogLevel("ERROR")

# Disable adaptive execution in streaming
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Read stream from Kafka
kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spotify_tracks") \
        .option("startingOffsets", "earliest") \
        .load()

# Extract JSON string from Kafka 'value'
string_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON string to structured data
json_df = string_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Duplicate records in streaming micro-batch
json_df = json_df.dropDuplicates(["artist", "track", "url"])

# Output to console
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

#aggregation
from pyspark.sql.functions import count

agg_df = json_df.groupBy("artist").agg(count("track").alias("track_count"))

query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
