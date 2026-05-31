from pyspark.sql import DataFrame
from pyspark.sql.functions import window, count, sum, lit
import state
from graphframes import GraphFrame

def getAggregatedData(df: DataFrame):
    return df \
        .groupBy(
        window(df.timestamp, "1 minute", "30 seconds"),
        df.action_type) \
        .agg(count("*").alias("action_count"), sum("price").alias("total_price"))

def getGraphFrame(raw_df):
    user_df = raw_df.select("user_id").distinct().withColumnRenamed("user_id", "id").withColumn("type", lit("USER"))
    seller_df = raw_df.select("seller_id").distinct().withColumnRenamed("seller_id", "id").withColumn("type", lit("SELLER"))
    product_df = raw_df.select("product_id").distinct().withColumnRenamed("product_id", "id").withColumn("type", lit("PRODUCT"))
    vertice_df = user_df.union(product_df).union(seller_df)

    user_actions = raw_df.filter(raw_df.action_type != "ACHAT") \
        .select(raw_df.user_id.alias("src"), raw_df.product_id.alias("dst"), raw_df.action_type.alias("relation"))

    seller_actions = raw_df.filter(raw_df.action_type == "ACHAT") \
        .select(raw_df.user_id.alias("src"), raw_df.seller_id.alias("dst"), raw_df.action_type.alias("relation"))

    product_actions = raw_df.filter(raw_df.action_type == "ACHAT") \
        .select(raw_df.user_id.alias("src"), raw_df.product_id.alias("dst"), raw_df.action_type.alias("relation"))

    edges_df = user_actions.union(seller_actions).union(product_actions)

    return GraphFrame(vertice_df, edges_df)

def processGraphBatch(batch_df, batch_id):
    graph = getGraphFrame(batch_df)

    new_edges = graph.edges.collect()
    state.all_edges.extend(new_edges)

    new_vertices = graph.vertices.collect()
    existing_ids = {n["id"] for n in state.all_vertices}
    for v in new_vertices:
        if v["id"] not in existing_ids:
            state.all_vertices.append(v)
            existing_ids.add(v["id"])