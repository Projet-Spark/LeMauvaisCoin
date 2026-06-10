from pyspark.sql import DataFrame
from pyspark.sql.functions import window, count, sum, lit, col
import state
from graphframes import GraphFrame

def getAggregatedData(df: DataFrame):
    return df \
        .withWatermark("timestamp", "30 seconds") \
        .groupBy(
        window(df.timestamp, "1 minute", "30 seconds"),
        df.action_type) \
        .agg(count("*").alias("action_count"), sum("price").alias("total_price"))

def processWindowBatch(batch_df, _batch_id):
    if batch_df.isEmpty():
        return
    for row in batch_df.collect():
        state.action_counts[row["action_type"]] = row["action_count"]

def getGraphFrame(raw_df):
    user_df = raw_df.select("user_id").distinct() \
        .withColumnRenamed("user_id", "id") \
        .withColumn("type", lit("USER")) \
        .withColumn("label", col("id"))
    seller_df = raw_df.select("seller_id").distinct() \
        .withColumnRenamed("seller_id", "id") \
        .withColumn("type", lit("SELLER")) \
        .withColumn("label", col("id"))
    product_df = raw_df.select("product_id").distinct() \
        .withColumnRenamed("product_id", "id") \
        .withColumn("type", lit("PRODUCT")) \
        .withColumn("label", col("id"))
    vertice_df = user_df.union(seller_df).union(product_df)

    user_product = raw_df \
        .select(raw_df.user_id.alias("src"), raw_df.product_id.alias("dst"), raw_df.action_type.alias("relation"))


    user_seller = raw_df.filter(raw_df.action_type == "ACHAT") \
        .select(raw_df.user_id.alias("src"), raw_df.seller_id.alias("dst"), raw_df.action_type.alias("relation"))

    seller_product = raw_df \
        .select(raw_df.seller_id.alias("src"), raw_df.product_id.alias("dst"), lit("PROPOSE").alias("relation"))

    edges_df = user_product.union(user_seller).union(seller_product)

    return GraphFrame(vertice_df, edges_df)

def computeGraphMetrics(graph):
    metrics = {}

    for row in graph.inDegrees.collect():
        metrics.setdefault(row["id"], {"inDegree": 0, "outDegree": 0, "pagerank": 0.0})
        metrics[row["id"]]["inDegree"] = row["inDegree"]

    for row in graph.outDegrees.collect():
        metrics.setdefault(row["id"], {"inDegree": 0, "outDegree": 0, "pagerank": 0.0})
        metrics[row["id"]]["outDegree"] = row["outDegree"]

    try:
        pr = graph.pageRank(resetProbability=0.15, maxIter=5)
        for row in pr.vertices.collect():
            metrics.setdefault(row["id"], {"inDegree": 0, "outDegree": 0, "pagerank": 0.0})
            metrics[row["id"]]["pagerank"] = round(row["pagerank"], 4)
    except Exception as e:
        print(f"PageRank skipped: {e}")

    return metrics

def processGraphBatch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    graph = getGraphFrame(batch_df)

    new_edges = graph.edges.dropDuplicates(["src", "dst", "relation"]).collect()
    existing_edge_keys = {(e["src"], e["dst"], e["relation"]) for e in state.all_edges}
    for e in new_edges:
        key = (e["src"], e["dst"], e["relation"])
        if key not in existing_edge_keys:
            state.all_edges.append(e)
            existing_edge_keys.add(key)

    new_vertices = graph.vertices.collect()
    existing_ids = {n["id"] for n in state.all_vertices}
    for v in new_vertices:
        if v["id"] not in existing_ids:
            state.all_vertices.append(v)
            existing_ids.add(v["id"])

    batch_metrics = computeGraphMetrics(graph)
    for node_id, m in batch_metrics.items():
        if node_id in state.graph_metrics:
            state.graph_metrics[node_id]["inDegree"] = state.graph_metrics[node_id].get("inDegree", 0) + m.get("inDegree", 0)
            state.graph_metrics[node_id]["outDegree"] = state.graph_metrics[node_id].get("outDegree", 0) + m.get("outDegree", 0)
            state.graph_metrics[node_id]["pagerank"] = m.get("pagerank", 0.0)
        else:
            state.graph_metrics[node_id] = dict(m)
