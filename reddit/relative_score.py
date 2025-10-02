from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# Parse a line of input JSON and extract subreddit and score.
def make_key_value(dic):
    subreddit = dic['subreddit'] # Extract subreddit name
    score = dic['score'] # Extract score value
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

# From (subreddit, (comment_dict, avg_score)) -> (relative_score, author)
def get_relative_avg(kv):
    subreddit, (comment_data, avg_score) = kv
    relative_avg = comment_data['score'] / avg_score
    return (relative_avg, comment_data['author'])

# Serialize output pair as JSON line
def make_json(kv):
    relative_avg, author = kv
    return json.dumps([relative_avg, author])

def main(inputs, output):
    # Read & parse, then cache (json.loads is expensive)
    comment_data = sc.textFile(inputs).map(json.loads).cache()

    # subreddit averages (ignore averages <= 0)
    comment_score = comment_data.map(make_key_value)
    total_by_sub = comment_score.reduceByKey(add_pairs)
    avg_by_sub = total_by_sub.mapValues(get_average)
    avg_pos_by_sub = avg_by_sub.filter(lambda c: c[1] > 0)

    # Pair by subreddit for join (subreddit, (comment_dict, avg))
    comment_by_sub = comment_data.map(lambda c: (c['subreddit'], c))
    # Inner join comments with subreddit averages (relative_score, author)
    joined_by_sub = comment_by_sub.join(avg_pos_by_sub)
    # Compute relative score & author
    relative_avg = joined_by_sub.map(get_relative_avg)
    # Sort by relative score DESC and save
    sorted_data = relative_avg.sortByKey(False).map(make_json)
    sorted_data.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)