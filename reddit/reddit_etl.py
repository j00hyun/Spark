from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# Extract only the required fields from the original dictionary
def select_fields_from_dict(dic):
    return {'subreddit': dic['subreddit'],
            'score': dic['score'],
            'author': dic['author']}

def main(inputs, output):

    # Read input file as RDD of strings, then parse JSON into Python dict
    dicts = sc.textFile(inputs).map(json.loads)

    # Keep only subreddit, score, and author fields
    comment = dicts.map(select_fields_from_dict)

    # Keep only rows where subreddit contains the letter 'e'
    comment_e = comment.filter(lambda c: 'e' in c['subreddit']).cache()

    # Split into positive (score > 0) and negative (score <= 0)
    pos_comment = comment_e.filter(lambda c: c['score'] > 0)
    neg_comment = comment_e.filter(lambda c: c['score'] <= 0)

    # Convert to JSON strings and save as text files in separate directories
    pos_comment.map(json.dumps).saveAsTextFile(output + '/positive')
    neg_comment.map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)