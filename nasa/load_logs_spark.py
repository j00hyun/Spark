#!/usr/bin/env python3
import sys
import re
from datetime import datetime
from pyspark.sql import SparkSession, Row, functions as F, types as T

# -------------------------------------------------------------
# Regular expression pattern for parsing log lines
# Captures: host, datetime, path, and number of bytes transferred
# -------------------------------------------------------------
line_re = re.compile(
    r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\S+)$'
)

"""
Parse one line of the log file.
Extract host, datetime, path, and bytes using regex.
Return a Row object or None if the line doesn't match.
"""
def parse_line(line):
    m = line_re.match(line)

    if not m:
        return None

    host, dt, path, bytes_str = m.groups()

    try:
        b = int(bytes_str)
    except ValueError:
        b = 0  # Handle "-" or invalid byte values

    # Convert datetime string like "01/Jul/1995:00:00:01" → Python datetime
    dt_parsed = datetime.strptime(dt, "%d/%b/%Y:%H:%M:%S")

    # Return a Row object containing the parsed fields
    return Row(host=host, datetime=dt_parsed, path=path, bytes=b)

# -------------------------------------------------------------
# Main logic: Read NASA logs → Parse → Convert to DataFrame →
# Write to Cassandra using spark-cassandra-connector
# -------------------------------------------------------------
def main(inputs, keyspace, table):
    # Read raw gzip logs from HDFS or local path
    lines = sc.textFile(inputs)

    # Parse and filter invalid lines
    parsed = lines.map(parse_line).filter(lambda r: r is not None)

    # Convert to DataFrame (columns: host, dt, path, bytes)
    df = spark.createDataFrame(parsed)

    # Add UUID for each record
    df = df.withColumn("id", F.expr("uuid()"))

    # Select columns in order (must match Cassandra table schema)
    final_df = df.select("host", "id", "datetime", "path", "bytes")

    # Repartition the DataFrame into 16 partitions to increase parallelism when writing to Cassandra
    final_df = df.repartition(16)

    # Write DataFrame to Cassandra
    final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .mode("append") \
        .save()

    print(f"Successfully loaded logs from {inputs} into {keyspace}.{table}")
    spark.stop()

# -------------------------------------------------------------
# Entry point
# -------------------------------------------------------------
if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]

    cluster_seeds = ['node1.local', 'node2.local']

    spark = SparkSession.builder.appName('load logs to cassandra') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
        .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(inputs, keyspace, table)
