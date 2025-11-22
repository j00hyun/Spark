import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions as F, types

# ---------------------------------------
# Simple Linear Regression Helper Columns
# ---------------------------------------
# We need to accumulate:
# sum_x, sum_y, sum_xy, sum_x2, n
# Then calculate:
#   beta  = (sum_xy - (sum_x * sum_y)/n) / (sum_x2 - (sum_x*sum_x)/n)
#   alpha = (sum_y/n) - beta * (sum_x/n)
# ---------------------------------------

def main(topic):
    spark = SparkSession.builder.appName('streaming regression').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # -------------------------------
    # 1. Read from Kafka stream
    # -------------------------------
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()

    # extract message string
    values = messages.select(
        messages['value'].cast('string').alias('msg')
    )

    # -------------------------------
    # 2. Parse "x y" into float columns
    # -------------------------------
    # msg looks like: "-740.844 -10829.371"
    parts = F.split(values['msg'], ' ')

    xy_df = values.select(
        parts.getItem(0).cast('float').alias('x'),
        parts.getItem(1).cast('float').alias('y')
    )

    # -------------------------------
    # 3. Compute aggregates needed for slope/intercept
    # -------------------------------
    agg_df = xy_df.groupBy().agg(
        F.sum('x').alias('sum_x'),
        F.sum('y').alias('sum_y'),
        F.sum( xy_df['x'] * xy_df['y'] ).alias('sum_xy'),
        F.sum( xy_df['x'] * xy_df['x'] ).alias('sum_x2'),
        F.count('*').alias('n')
    )

    # -------------------------------
    # 4. Compute slope & intercept
    # -------------------------------
    # beta = (sum_xy - sum_x*sum_y/n) / (sum_x2 - (sum_x^2)/n)
    beta = (
        (F.col('sum_xy') - (F.col('sum_x') * F.col('sum_y') / F.col('n')))
        /
        (F.col('sum_x2') - (F.col('sum_x') * F.col('sum_x') / F.col('n')))
    )

    # alpha = (sum_y/n) - beta*(sum_x/n)
    alpha = (
        (F.col('sum_y') / F.col('n'))
        - beta * (F.col('sum_x') / F.col('n'))
    )

    result_df = agg_df.select(
        beta.alias('slope'),
        alpha.alias('intercept')
    )

    # -------------------------------
    # 5. Output to console
    # -------------------------------
    stream = result_df.writeStream.format('console') \
            .outputMode('complete') \
            .start()

    stream.awaitTermination(600)     # run 10 minutes


if __name__ == '__main__':
    topic = sys.argv[1]          # e.g. xy-1
    main(topic)
