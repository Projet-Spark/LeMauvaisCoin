import dataclasses
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from generateur import Event

SPARK_TYPE_MAP = {str: StringType(), float: FloatType()}

def dataclass_to_spark_schema(dc):
    return StructType([
        StructField(f.name, SPARK_TYPE_MAP[f.type])
        for f in dataclasses.fields(dc)
    ])

spark = SparkSession.builder.appName("LeMauvaisCoin").getOrCreate()

def getTcpData():
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    schema = dataclass_to_spark_schema(Event)
    return lines.select(from_json(col("value"), schema).alias("data")).select("data.*")
