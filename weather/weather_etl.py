import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as F, types as T

# Explicit schema for the GHCN CSV (so column names/types are exactly as expected)
observation_schema = T.StructType([
    T.StructField('station',     T.StringType()),
    T.StructField('date',        T.StringType()),   # format: YYYYMMDD
    T.StructField('observation', T.StringType()),   # e.g., 'TMAX', 'TMIN', ...
    T.StructField('value',       T.IntegerType()),  # scaled by 10 for temps
    T.StructField('mflag',       T.StringType()),
    T.StructField('qflag',       T.StringType()),   # quality flag (null = good)
    T.StructField('sflag',       T.StringType()),
    T.StructField('obstime',     T.StringType()),
])

def main(inputs, output):
    # Read compressed CSV(s) into a DataFrame using the explicit schema
    weather = spark.read.csv(inputs, schema=observation_schema)

    # Keep only: good quality rows (qflag is null) + Canadian stations (CA*) + TMAX observations
    filtered = (
        weather
            .where(F.col("qflag").isNull())             # only quality-ok records
            .where(F.col("station").startswith("CA"))   # Canada stations
            .where(F.col("observation") == "TMAX")      # maximum temperature
    )

    # Convert tenths of °C to °C: tmax = value / 10.0
    with_tmax = filtered.withColumn("tmax", F.col("value") / F.lit(10.0))
    # Keep only the columns required for downstream work
    etl_data = with_tmax.select("station", "date", "tmax")
    # Write newline-delimited JSON, GZIP-compressed, overwriting any previous output
    etl_data.write.json(output, compression="gzip", mode="overwrite")

    # Cleanly stop the SparkSession
    spark.stop()

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)