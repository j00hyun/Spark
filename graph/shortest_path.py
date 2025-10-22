import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

# -------------------------------
# Define Schemas for DataFrames
# -------------------------------
edge_schema = T.StructType([
    T.StructField("src", T.IntegerType()), # source node
    T.StructField("dst", T.IntegerType()), # destination node
])

path_schema = T.StructType([
    T.StructField("node",   T.IntegerType()), # current node
    T.StructField("source", T.IntegerType()), # where this node was reached from
    T.StructField("dist",   T.IntegerType()), # distance (steps) from the start node
])

# -------------------------------
# Parse input lines from text file
# Example: "1: 3 5" → (1, [3, 5])
# -------------------------------
def parse_line(line):
    line = line.strip()

    left, right = line.split(':')
    left = left.strip()
    right = right.strip()

    node = int(left)

    if right == '':
        neighbors = []
    else:
        parts = right.split(' ')
        neighbors = list(map(int, parts))

    return (node, neighbors)

# -------------------------------
# Expand adjacency list into individual edges
# Example: (1, [3,5]) → [(1,3), (1,5)]
# -------------------------------
def expand_edges(record):
    node, neighbors = record
    return [(node, neighbor) for neighbor in neighbors]

# -------------------------------
# Save intermediate results after each iteration
# Output format: node \t source \t dist
# -------------------------------
def save_iter_snapshot(df, output_dir, i):
    (df
     .select(F.concat_ws("\t",
                         F.col("node").cast("string"),
                         F.col("source").cast("string"),
                         F.col("dist").cast("string")).alias("value"))
     .coalesce(1)
     .write.mode("overwrite")
     .text(output_dir + '/iter-' + str(i))
    )

# -------------------------------
# Reconstruct the shortest path by tracing back from destination to source
# -------------------------------
def reconstruct_path_df(spark, known_df, src, dst, output_dir):
    known_df = known_df.cache() # Cache for repeated access

    path = []
    cur = int(dst)
    sc = spark.sparkContext

    for _ in range(7): # Max depth = 6 → up to 7 nodes
        # Find the record for the current node
        rows = (known_df
            .where(F.col("node") == cur)
            .select("source", "dist")
            .limit(1)
            .collect())

        # Stop if no such node exists
        if not rows:
            break

        src_node = rows[0]["source"]
        path.append(cur)

        # Stop if we reached the source node
        if cur == src:
            break

        cur = int(src_node)

    # Reverse the path to go from source → destination
    path.reverse()

    # Save the reconstructed path to output directory
    sc.parallelize([str(x) for x in path]) \
        .coalesce(1) \
        .saveAsTextFile(output_dir + '/path')

# -------------------------------
# Main BFS Algorithm
# -------------------------------
def main(input_dir, output_dir, src, dst):
    # Load the graph file from input directory
    lines = sc.textFile(input_dir + "/links-simple-sorted.txt")

    # Parse and flatten adjacency list into edge pairs
    parsed = lines.map(parse_line)
    edges_rdd = parsed.flatMap(expand_edges)

    # Create DataFrame of all edges
    edges_df = spark.createDataFrame(edges_rdd, schema=edge_schema).cache()

    # Initialize known paths (start node)
    known_df = spark.createDataFrame([(src, -1, 0)], schema=path_schema).cache()
    frontier_df = known_df.cache() # Start frontier (nodes to expand next)

    # ----------------------------------
    # BFS Iterations (max 6 levels deep)
    # ----------------------------------
    for i in range(6):
        # Save current known paths for debugging
        save_iter_snapshot(known_df, output_dir, i)

        # Stop early if destination is already found
        if known_df.where(F.col("node") == dst).limit(1).count() == 1:
            break

        # Expand frontier: find neighbors of current frontier nodes
        cand = (frontier_df.alias("f")
            .join(edges_df.alias("e"), F.col("f.node") == F.col("e.src"), "inner")
            .select(F.col("e.dst").alias("node"),
                    F.col("f.node").alias("source"),
                    (F.col("f.dist") + F.lit(1)).alias("dist")))

        # Within this iteration, for each node, keep only the shortest (lowest dist)
        w_node = Window.partitionBy("node").orderBy(F.col("dist").asc())
        best_cand = (cand
            .withColumn("rn", F.row_number().over(w_node))
            .where(F.col("rn") == 1)
            .drop("rn"))

        # Keep only nodes that are either new or reached with a shorter path
        newer = (best_cand.alias("n")
            .join(known_df.alias("k"), F.col("n.node") == F.col("k.node"), "left")
            .where(F.col("k.node").isNull() | (F.col("n.dist") < F.col("k.dist")))
            .select(F.col("n.node").alias("node"),
                    F.col("n.source").alias("source"),
                    F.col("n.dist").alias("dist"))
        ).cache()

        # Combine known paths and new candidates, keeping only the shortest per node
        all_paths = known_df.union(newer)
        known_df = (all_paths
            .withColumn("rn", F.row_number().over(w_node))
            .where(F.col("rn") == 1)
            .drop("rn")
            .cache())

        # Update frontier for next iteration
        frontier_df = newer

    # Save the final known paths for verification
    save_iter_snapshot(known_df, output_dir, "final")

    # Trace and save the final shortest path
    reconstruct_path_df(spark, known_df, src, dst, output_dir)

    spark.stop()

if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    src = int(sys.argv[3])
    dst = int(sys.argv[4])
    spark = SparkSession.builder.appName('shortest path').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_dir, output_dir, src, dst)