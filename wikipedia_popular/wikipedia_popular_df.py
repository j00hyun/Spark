import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as F, types as T

# Define an explicit schema for the Wikipedia pagecounts dataset
schema = T.StructType([
    T.StructField('language',   T.StringType()), # language
    T.StructField('title',      T.StringType()), # page title
    T.StructField('views',      T.LongType()),   # number of views
    T.StructField('bytes',      T.LongType()),   # bytes transferred
])

# UDF (User Defined Function) to extract the hour from the input file path
# Example: '/courses/732/pagecounts-20160801-120000.gz' → '20160801-12'
@F.udf(returnType=T.StringType())
def path_to_hour(path):
    filename = path.split('/')[-1]              # pagecounts-20160801-120000.gz
    day_hour = filename[len('pagecounts-'):len('pagecounts-')+11]  # '20160801-12'
    return day_hour

def main(inputs, output):
    # Read space-delimited Wikipedia pagecount files with the defined schema
    wiki = spark.read.csv(inputs, sep=' ', schema=schema)

    # Add a column with the file path and extract hour information using the UDF
    wiki = wiki.withColumn('filename', F.input_file_name())
    wiki = wiki.withColumn('hour', path_to_hour(F.col('filename')))

    # Filter for English-language pages only,
    # excluding the 'Main_Page' and pages starting with 'Special:'
    filtered = wiki.where(
        (wiki.language == 'en') &
        (wiki.title != 'Main_Page') &
        (~wiki.title.startswith('Special:'))
    ).cache() # Cache since this DataFrame will be used multiple times

    # Compute the maximum number of views per hour
    max_views = filtered.groupBy('hour').agg(F.max('views').alias('max_views'))

    # Join the filtered data with the hourly maximums to find the most-viewed page(s)
    # If there’s a tie (multiple pages with the same view count), keep them all
    # Use the broadcast hint to tell Spark to replicate the small 'max_views' DataFrame across all nodes for a faster join
    most_viewed_pages = filtered.join(max_views.hint("broadcast"), on='hour') \
                                .where(filtered.views == max_views.max_views)

    # Select only relevant columns and sort by hour (and title for ties)
    result = most_viewed_pages.select('hour', 'title', 'views') \
                              .orderBy('hour', 'title')

    # Display the logical and physical execution plan in the console
    result.explain()

    # Write the output as newline-delimited JSON files
    result.write.json(output, mode='overwrite')

    # Stop the Spark session cleanly
    spark.stop()

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)