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
rds_endpoint = os.getenv('rds_endpoint')
rds_port = os.getenv('rds_port')
rds_username = os.getenv('rds_username')
rds_password = os.getenv('rds_password')
rds_db_name = os.getenv('rds_db_name')

db_properties = {
    "user": rds_username,
    "password": rds_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

def write_to_rds(batch_df, batch_id):
    print("URL",f"jdbc:mysql://{rds_endpoint}/{rds_db_name}")
    print("User",f"{rds_username}")
    print("Password", f"{rds_password}")

    # Write DataFrame data to the table (existing or newly created)
    dfwriter = batch_df.write.mode("append")
    dfwriter.jdbc(url=f"jdbc:mysql://{rds_endpoint}/{rds_db_name}", table="stock_data", properties=db_properties, mode="append")

# Write data to RDS
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_rds) \
    .start()
        
# Write data to RDS
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_rds) \
    .start()

query.awaitTermination()
