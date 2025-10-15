import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as F, types as T

# Define the explicit schema for Reddit comments
comments_schema = T.StructType([
    T.StructField('archived',               T.BooleanType()),
    T.StructField('author',                 T.StringType()),
    T.StructField('author_flair_css_class', T.StringType()),
    T.StructField('author_flair_text',      T.StringType()),
    T.StructField('body',                   T.StringType()),
    T.StructField('controversiality',       T.LongType()),
    T.StructField('created_utc',            T.StringType()),
    T.StructField('distinguished',          T.StringType()),
    T.StructField('downs',                  T.LongType()),
    T.StructField('edited',                 T.StringType()),
    T.StructField('gilded',                 T.LongType()),
    T.StructField('id',                     T.StringType()),
    T.StructField('link_id',                T.StringType()),
    T.StructField('name',                   T.StringType()),
    T.StructField('parent_id',              T.StringType()),
    T.StructField('retrieved_on',           T.LongType()),
    T.StructField('score',                  T.LongType()),
    T.StructField('score_hidden',           T.BooleanType()),
    T.StructField('subreddit',              T.StringType()),
    T.StructField('subreddit_id',           T.StringType()),
    T.StructField('ups',                    T.LongType()),
    T.StructField('year',                   T.IntegerType()),
    T.StructField('month',                  T.IntegerType()),
])

def main(inputs, output):
    # Read the Reddit comments JSON file using the defined schema
    reddit = spark.read.json(inputs, schema=comments_schema)

    # Select only the columns needed for this task
    filtered_reddit = (
        reddit
            .select("subreddit", "score")
            .where(F.col("subreddit").isNotNull() & (F.col("subreddit") != ""))
            .where(F.col("score").isNotNull())
    )

    # Group by subreddit and calculate the average score for each
    avg_reddit = (
        filtered_reddit.groupBy("subreddit")
                       .agg(F.avg("score").alias("avg_score"))
    )

    # Display the logical and physical execution plan in the console
    avg_reddit.explain()

    # Write the result as CSV files to the output directory
    avg_reddit.write.csv(output, mode='overwrite')

    # Stop the Spark session cleanly
    spark.stop()

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('reddit average df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)