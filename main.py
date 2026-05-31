import threading
from generateur import launchServer
from data_streaming import startSpark, getTcpData
from data_transformation import getAggregatedData

if __name__ == "__main__":
    ready = threading.Event()
    server_thread = threading.Thread(target=launchServer, args=(ready,), daemon=True)
    server_thread.start()
    ready.wait()

    spark = startSpark()
    df = getTcpData(spark)
    aggregated_df = getAggregatedData(df)
    aggregated_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="5 seconds") \
        .start() \
        .awaitTermination()