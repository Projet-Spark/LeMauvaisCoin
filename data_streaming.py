import dataclasses
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

EVENT_SCHEMA = StructType([
    StructField("timestamp",   TimestampType()),
    StructField("user_id",     StringType()),
    StructField("user_city",   StringType()),
    StructField("product_id",  StringType()),
    StructField("product_cat", StringType()),
    StructField("seller_id",   StringType()),
    StructField("action_type", StringType()),
    StructField("price",       DoubleType()),
])
def startSpark() -> SparkSession:
    spark = SparkSession.builder \
        .appName("LeMauvaisCoin") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   
    return spark 


def getTcpData(spark: SparkSession) -> DataFrame:
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    return lines.select(from_json(col("value"), EVENT_SCHEMA).alias("data")).select("data.*").withWatermark("timestamp", "5 minutes")
