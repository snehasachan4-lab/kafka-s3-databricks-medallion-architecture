from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("product", StringType()),
    StructField("amount", IntegerType()),
    StructField("payment_status", StringType()),
    StructField("timestamp", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
