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

# Use broadcast averages to compute (relative_score, author) for one comment
def relative_from_bcast(comment, avg_bcast):
    avg = avg_bcast.value.get(comment['subreddit']) # lookup avg for this subreddit
    if avg is None: # no average available → skip
        return None
    return (comment['score'] / avg, comment['author'])

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

    # Collect the small averages RDD to the driver and broadcast to executors.
    avg_dict = dict(avg_pos_by_sub.collect()) # subreddit keys ≤ 200k (~tens of MB) → safe to collect
    avg_bcast = sc.broadcast(avg_dict) # ex) {subreddit: average score}

    # Map over the large comments RDD and do a local lookup against the broadcast dict (map-side join; no shuffle).
    # Drop comments whose subreddit has no average.
    relative_avg = (
        comment_data
            .map(lambda c: relative_from_bcast(c, avg_bcast))  # (relative_score, author) or None
            .filter(lambda x: x is not None)
    )

    # Sort by relative score DESC and save
    sorted_data = relative_avg.sortByKey(False).map(make_json)
    sorted_data.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score broadcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)