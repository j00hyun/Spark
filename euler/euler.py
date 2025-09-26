from pyspark import SparkConf, SparkContext
import sys
import random

# add more functions as necessary

# this function is applied once per partition
def per_partition(it):
    # seed is made once per partition -> don't produce the same random sequence
    random.seed()

    total_iters = 0

    # iterate over all sample elements in that partition
    for _ in it:
        s = 0.0
        n = 0
        # draw random numbers until the partial sum reaches 1.0
        while s < 1.0:
            s += random.random()
            n += 1
        # accumulate this sample's draw count into the partition total
        total_iters += n
    # emit single value
    yield total_iters

def main(input_num):
    # main logic starts here

    # number of samples = 2^input_num
    samples = 1 << input_num

    # build an RDD with exactly 'samples' elements: 0 .. samples-1
    rdd = sc.range(0, samples, numSlices=10)
    # per_partition() returns one integer per partition
    # sum() aggregates those partition sums
    total_iterations = rdd.mapPartitions(per_partition).sum()
    # average number of draws per sample
    print(total_iterations / float(samples))

    # always stop the SparkContext when done
    sc.stop()

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    input_num = int(sys.argv[1])
    main(input_num)