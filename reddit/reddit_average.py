from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

# Parse a line of input JSON and extract subreddit and score.
def make_key_value(line):
    parsed_dict = json.loads(line) # Convert JSON string to Python dict
    subreddit = parsed_dict["subreddit"] # Extract subreddit name
    score = parsed_dict["score"] # Extract score value
    return (subreddit, (1, score)) # Return as key-value pair

# Add two (count, score_sum) pairs together.
def add_pairs(x, y):
    x_cnt, x_score = x
    y_cnt, y_score = y
    return (x_cnt + y_cnt, x_score + y_score)

# Compute the average score from a (count, score_sum) pair.
def get_average(value):
    count, score_sum = value
    return score_sum / count

# Convert (subreddit, average_score) into a JSON string.
def make_json(kv):
    subreddit, avg_score = kv
    return json.dumps([subreddit, avg_score])

def main(inputs, output):
    # main logic starts here

    # Step 1: Read input file(s) into an RDD of lines (strings)
    text = sc.textFile(inputs)
    # Step 2: Parse JSON lines → (subreddit, (1, score))
    key_values = text.map(make_key_value)
    # Step 3: Reduce by subreddit → (subreddit, (total_count, total_score_sum))
    totals = key_values.reduceByKey(add_pairs)
    # Step 4: Map values to average score → (subreddit, average_score)
    averages = totals.mapValues(get_average)
    # Step 5: Convert each pair to a JSON string
    json_data = averages.map(make_json)
    # Step 6: Save output as text files (one line per record, multiple part files)
    outdata = json_data.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)