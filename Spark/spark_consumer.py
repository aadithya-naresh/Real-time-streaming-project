from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

# Create a Spark session
spark = SparkSession.builder \
    .appName("StockDataConsumer") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", StringType(), True),  # Storing timestamp as string to match your format
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True)
])

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data
df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Write data to console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
