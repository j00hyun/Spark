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

    # Filter out invalid records (non-null qflag = bad data)
    # Keep only relevant columns and cache the DataFrame for reuse
    base = (weather
        .where(F.col('qflag').isNull())
        .select('station', 'date', 'observation', 'value')
        .cache())

    # Extract maximum daily temperature (TMAX)
    # Group by date/station to ensure one record per station per day
    # Convert tenths of °C → °C by dividing by 10
    max_temp = (base
        .where(F.col('observation') == 'TMAX')
        .groupBy('date', 'station')
        .agg(F.max('value').alias('max_value'))
        .select('station', 'date', (F.col('max_value') / F.lit(10.0)).alias('max_value')))

    # Extract minimum daily temperature (TMIN)
    # Use min() since we want the lowest temperature per station per day
    # Convert to °C in the same way
    min_temp = (base
                .where(F.col('observation') == 'TMIN')  # minimum temperature
                .groupBy('date', 'station')
                .agg(F.max('value').alias('min_value'))
                .select('station', 'date', (F.col('min_value') / F.lit(10.0)).alias('min_value')))

    # Join TMAX and TMIN DataFrames to compute the temperature range for each station/day
    # Cache the result since it will be reused in the next aggregation
    temp_range = (max_temp
        .join(min_temp, on=['date', 'station'], how='inner')
        .withColumn('range', F.col('max_value') - F.col('min_value'))
        .cache())

    # For each date, find the largest temperature range among all stations
    max_temp_range = (temp_range
        .groupBy('date')
        .agg(F.max('range').alias('max_range')))

    # Join again to identify which station(s) had that maximum daily range
    # Use broadcast hint for efficiency, since max_temp_range is small
    result = (temp_range
        .join(max_temp_range.hint('broadcast'), on='date')
        .where(F.col('range') == F.col('max_range'))
        .select('date', 'station', F.round('range', 1).alias('range'))
        .orderBy('date', 'station'))

    # Write the final result as CSV files (Spark default partitioned format)
    result.write.csv(output, mode='overwrite')

    # Stop Spark session to free resources
    spark.stop()

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
