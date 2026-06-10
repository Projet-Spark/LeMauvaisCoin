import threading
from generateur import launchServer
from data_streaming import startSpark, getTcpData
from data_transformation import getAggregatedData, processWindowBatch, processGraphBatch
from dashboard import app

if __name__ == "__main__":
    ready = threading.Event()
    server_thread = threading.Thread(target=launchServer, args=(ready,), daemon=True)
    server_thread.start()
    ready.wait()

    dash_thread = threading.Thread(target=app.run, daemon=True)
    dash_thread.start()

    spark = startSpark()
    df = getTcpData(spark)
    aggregated_df = getAggregatedData(df)
    query1 = aggregated_df.writeStream \
        .outputMode("update") \
        .foreachBatch(processWindowBatch) \
        .option("checkpointLocation", "/tmp/checkpoint_window") \
        .trigger(processingTime="5 seconds") \
        .start()

    query2 = df.writeStream \
        .foreachBatch(processGraphBatch) \
        .trigger(processingTime="5 seconds") \
        .start()

    spark.streams.awaitAnyTermination()