from pyspark import SparkConf, SparkContext
import sys
import re, string

# regex: split by punctuation + whitespace
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

inputs = sys.argv[1] # input file path
output = sys.argv[2] # output directory

conf = SparkConf().setAppName('word count improved') # configure and start a Spark context named word count
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

# used to add up counts when reducing by key
def add(x, y):
    return x + y

# extract the key (word) from (word, count) tuple
def get_key(kv):
    return kv[0]

# converts (word, count) into a string like "word count"
def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs) # reads the input text file(s) into an RDD

# repartition to increase parallelism (e.g., 64 partitions)
text = text.repartition(64)
# split lines into words using regax
tokens = text.flatMap(lambda line: wordsep.split(line))
# convert all words to lowercase
tokens_lower = tokens.map(lambda w: w.lower())
# remove empty strings
tokens_clean = tokens_lower.filter(lambda w: len(w) > 0)
# map words to (word, 1) and sum counts by key
wordcount = tokens_clean.map(lambda w: (w, 1)).reduceByKey(add)

outdata = wordcount.sortBy(get_key).map(output_format) # sorts results by word alphabetically, then format them as strings
outdata.saveAsTextFile(output) # saves the final results as text files in the given output directory
