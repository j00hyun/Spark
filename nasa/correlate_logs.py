import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from math import sqrt
from pyspark.sql import SparkSession, Row, functions as F, types as T

# Regular expression pattern for parsing log lines
# Captures: host, datetime, path, and number of bytes transferred
line_re = re.compile(
    r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$'
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
    b = int(bytes_str) # Convert bytes to integer
    return Row(host=host, dt=dt, path=path, bytes=b)

def main(input):
    # Read input logs from the provided directory
    lines = sc.textFile(input)

    # Parse each line, keep only valid parsed results
    parsed = lines.map(parse_line).filter(lambda r: r is not None)

    # Convert parsed RDD to DataFrame (columns: host, dt, path, bytes)
    df = spark.createDataFrame(parsed)

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
            F.sum("one").alias("n"),    # total number of hosts
            F.sum("x").alias("sx"),     # Σx
            F.sum("y").alias("sy"),     # Σy
            F.sum("x2").alias("sx2"),   # Σx²
            F.sum("y2").alias("sy2"),   # Σy²
            F.sum("xy").alias("sxy"),   # Σxy
        )
        .collect()[0]) # Collect as a Row object

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
    r2 = r * r # Coefficient of determination

    # Print final results
    print(f"r = {r:.6f}")
    print(f"r^2 = {r2:.6f}")

    # Stop the Spark session
    spark.stop()

if __name__ == '__main__':
    input = sys.argv[1]
    spark = SparkSession.builder.appName('nasa correlate logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input)