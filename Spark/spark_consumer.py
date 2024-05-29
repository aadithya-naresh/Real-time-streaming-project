import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

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
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data
df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# RDS configuration
rds_endpoint = os.getenv('RDS_ENDPOINT')
rds_port = os.getenv('RDS_PORT')
rds_username = os.getenv('RDS_USERNAME')
rds_password = os.getenv('RDS_PASSWORD')
rds_db_name = os.getenv('RDS_DB_NAME')

def write_to_rds(batch_df, batch_id):

    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{rds_endpoint}/{rds_db_name}") \
        .option("dbtable", "stock_data") \
        .option("user", rds_username) \
        .option("password", rds_password) \
        .mode("append") \
        .save()
        
# Write data to RDS
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_rds) \
    .start()

query.awaitTermination()
