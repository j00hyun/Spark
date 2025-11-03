#!/usr/bin/env python3
import sys
from math import sqrt
from pyspark.sql import SparkSession, functions as F

def main(keyspace, table):
    # Read data from Cassandra
    df = (spark.read.format("org.apache.spark.sql.cassandra")
          .options(table=table, keyspace=keyspace)
          .load())

    # Select only the required columns
    df = df.select("host", "bytes")

    # Aggregate by host:
    # x = number of requests per host
    # y = total bytes sent per host
    by_host = (df.
        groupBy("host")
        .agg(
        F.count(F.lit(1)).alias("x"),
        F.sum("bytes").alias("y")
        ))

    # Add derived columns for computing correlation components
    # x², y², xy, and a constant column (1) for counting n
    enriched = (by_host
                .withColumn("x2", F.col("x") * F.col("x"))
                .withColumn("y2", F.col("y") * F.col("y"))
                .withColumn("xy", F.col("x") * F.col("y"))
                .withColumn("one", F.lit(1))
    )

    # Aggregate across all hosts to get the six required sums
    sums = (enriched.
        agg(
        F.sum("one").alias("n"),  # total number of hosts
        F.sum("x").alias("sx"),  # Σx
        F.sum("y").alias("sy"),  # Σy
        F.sum("x2").alias("sx2"),  # Σx²
        F.sum("y2").alias("sy2"),  # Σy²
        F.sum("xy").alias("sxy"),  # Σxy
        )
        .collect()[0])  # Collect as a Row object

    # Extract the six summed values and convert to float
    n = float(sums["n"])
    sx = float(sums["sx"])
    sy = float(sums["sy"])
    sx2 = float(sums["sx2"])
    sy2 = float(sums["sy2"])
    sxy = float(sums["sxy"])

    # Compute the correlation coefficient 'r'
    # Formula:
    # r = [nΣxy - (Σx)(Σy)] / sqrt([nΣx² - (Σx)²] [nΣy² - (Σy)²])
    num = n * sxy - (sx * sy)
    den_x = n * sx2 - (sx * sx)
    den_y = n * sy2 - (sy * sy)
    denom = sqrt(den_x) * sqrt(den_y)
    r = num / denom
    r2 = r * r

    # Print final results
    print(f"r = {r:.6f}")
    print(f"r^2 = {r2:.6f}")

if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]

    cluster_seeds = ['node1.local', 'node2.local']
    spark = (SparkSession.builder
        .appName('correlate logs cassandra')
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds))
        .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions')
        .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    main(keyspace, table)
    spark.stop()
