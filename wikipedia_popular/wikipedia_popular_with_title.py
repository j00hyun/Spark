from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1] # input file path
output = sys.argv[2] # output directory

# configure and start a Spark context named wikipedia popular
conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

# Split a line of input into a tuple of five fields
def make_tuple(line):
    time, lang, title, view, size = line.split()
    return (time, lang, title, view, size)

# Convert the numeric fields (view, size) from string to integer
def change_type_of_tuple(tpl):
    time, lang, title, view, size = tpl
    return (time, lang, title, int(view), int(size))

# Filter out records that we do not want to include
def filter_record(tpl):
    time, lang, title, view, size = tpl
    if lang != 'en': return False # keep only English Wikipedia
    if title == 'Main_Page': return False # exclude the main page
    if title.startswith('Special:'): return False # exclude "Special:" pages
    return True

# Transform into (time, (view, title)) key-value pairs
def make_key_value(tpl):
    time, lang, title, view, size = tpl
    return (time, (view, title))

# Reducer function: find the maximum view count for each time
def find_max_views_per_time(x, y):
    return max(x[0], y[0])

# Extract the key (time) from a (time, (view, title)) pair
def get_key(kv):
    return kv[0]

# Format output as tab-separated string
def tab_separated(kv):
    (time, (view, title)) = kv
    return "%s\t(%s, '%s')" % (time, view, title)

text = sc.textFile(inputs) # reads the input text file(s) into an RDD

# Parse each line into a tuple (time, lang, title, view, size as strings)
tuples = text.map(make_tuple)
# Convert view and size into integers
tuples_change_type = tuples.map(change_type_of_tuple)
# Filter out non-English, Main_Page, and Special: pages
tuples_filter = tuples_change_type.filter(filter_record)
# Create (time, (view, title)) key-value pairs
key_values = tuples_filter.map(make_key_value)
# Find the maximum view count for each time
max_views = key_values.reduceByKey(find_max_views_per_time)

# sort results by time
outdata = max_views.sortBy(get_key)
# Format as tab-separated lines and save to output directory
outdata = outdata.map(tab_separated).saveAsTextFile(output)
