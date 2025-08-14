from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType, StringType

# Define the schema for your JSON data
schema = StructType() \
    .add("artist", StringType()) \
    .add("track", StringType()) \
    .add("url", StringType())

# Start Spark session with required AWS packages
spark = SparkSession.builder \
    .appName("KafkaToS3Streaming") \
    .config("spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4",
                "org.apache.kafka:kafka-clients:3.9.0",
                "org.lz4:lz4-java:1.8.0",
                "org.xerial.snappy:snappy-java:1.1.10.5",
                "org.apache.commons:commons-pool2:2.11.1",
                "org.apache.hadoop:hadoop-aws:3.3.2"
            ])) \
    .getOrCreate()

spark = SparkSession.builder \
    .appName("KafkaToS3Streaming") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .getOrCreate()


# Set logging level
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.adaptive.enabled", "false")  # Disable adaptive execution in streaming

# Set AWS S3 credentials (not for production use)
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "Youe s3 access key")
hadoop_conf.set("fs.s3a.secret.key", "Your s3 secret key")
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Read stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "spotify_tracks") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka 'value' to string and parse JSON
string_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
json_df = string_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Deduplicate streaming data
dedup_df = json_df.dropDuplicates(["artist", "track", "url"])


s3_query = dedup_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "s3a://your bucket name/spark_tracks_csv/") \
    .option("checkpointLocation", "s3a://your bucket name/spotify_data_csv/checkpoints/") \
    .option("header", "true") \
    .start()

# Await termination of all streams
s3_query.awaitTermination()
