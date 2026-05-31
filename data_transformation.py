from pyspark.sql import DataFrame
from pyspark.sql.functions import window, count, sum

def getAggregatedData(df: DataFrame):
    return df \
        .groupBy(
        window(df.timestamp, "1 minute", "30 seconds"),
        df.action_type) \
        .agg(count("*").alias("action_count"), sum("price").alias("total_price"))