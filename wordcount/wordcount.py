from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1] # input file path
output = sys.argv[2] # output directory

conf = SparkConf().setAppName('word count') # configure and start a Spark context named word count
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

# split a line into words and yields (word, 1) pairs
def words_once(line):
    for w in line.split():
        yield (w, 1)

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
words = text.flatMap(words_once) # splits lines into individual (word, 1) pairs
wordcount = words.reduceByKey(add) # groups by word and sums the counts -> (word, total_count)

outdata = wordcount.sortBy(get_key).map(output_format) # sorts results by word alphabetically, then format them as strings
outdata.saveAsTextFile(output) # saves the final results as text files in the given output directory
