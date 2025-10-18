import sys
assert sys.version_info >= (3, 5)  # Ensure Python 3.5+

from pyspark.sql import SparkSession, types as T

# Explicit schema for the GHCN CSV
observation_schema = T.StructType([
    T.StructField('station',     T.StringType()),
    T.StructField('date',        T.StringType()),   # YYYYMMDD
    T.StructField('observation', T.StringType()),   # TMAX, TMIN, ...
    T.StructField('value',       T.IntegerType()),  # tenths of °C for temps
    T.StructField('mflag',       T.StringType()),
    T.StructField('qflag',       T.StringType()),   # null = good
    T.StructField('sflag',       T.StringType()),
    T.StructField('obstime',     T.StringType()),
])

def main(inputs, output):

    # Read CSV with explicit schema and register a temp view
    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.createOrReplaceTempView("weather")

    # Base: keep only quality-ok rows and the columns we need
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW base AS
        SELECT
            station,
            date,
            observation,
            value
        FROM weather
        WHERE qflag IS NULL
    """)
    spark.sql("CACHE TABLE base")

    # Daily TMAX per station
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW max_temp AS
        SELECT
            date,
            station,
            ( MAX(value) / 10.0 ) AS max_value
        FROM base
        WHERE observation = 'TMAX'
        GROUP BY date, station
    """)

    # Daily TMIN per station
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW min_temp AS
        SELECT
            date,
            station,
            ( MIN(value) / 10.0 ) AS min_value
        FROM base
        WHERE observation = 'TMIN'
        GROUP BY date, station
    """)

    # Join TMAX/TMIN and compute range in °C
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW temp_range AS
        SELECT
            mt.date,
            mt.station,
            (mt.max_value - nt.min_value) AS range
        FROM max_temp mt
        INNER JOIN min_temp nt
            ON mt.date = nt.date AND mt.station = nt.station
    """)
    spark.sql("CACHE TABLE temp_range")

    # Per-day maximum range
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW max_temp_range AS
        SELECT
            date,
            MAX(range) AS max_range
        FROM temp_range
        GROUP BY date
    """)

    # Join back to find which station(s) achieved the per-day max.
    # Use a broadcast hint on the small max_temp_range.
    result = spark.sql("""
        SELECT /*+ BROADCAST(mtr) */
            tr.date,
            tr.station,
            ROUND(tr.range, 1) AS range
        FROM temp_range tr
        INNER JOIN max_temp_range mtr
            ON tr.date = mtr.date
        WHERE tr.range = mtr.max_range
        ORDER BY tr.date, tr.station
    """)

    # Write CSV output
    result.write.mode("overwrite").csv(output)

    spark.stop()

if __name__ == "__main__":
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range SQL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    main(inputs, output)
