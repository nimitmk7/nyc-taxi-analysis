from __future__ import print_function

import sys
from pyspark import SparkContext

def construct_pairs(list):
    key = list[0]
    value = list[1:]
    value = ",".join(value)
    return (key, value)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Not enough arguments", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    rdd_fares = sc.textFile(sys.argv[1], 4)
    rdd_license = sc.textFile(sys.argv[2], 4)

    # Transforming RDD into list of lists, where each list is a row in the file
    license_list = rdd_license.map(lambda x: x.split(',')).filter(lambda x: ('medallion' not in x))
    fares_list = rdd_fares.map(lambda x: x.split(',')).filter(lambda x: ('medallion' not in x))

    license_pairs = license_list.map(lambda x: construct_pairs(x))
    fare_pairs = fares_list.map(lambda x: construct_pairs(x))

    joined_pairs = fare_pairs.join(license_pairs).mapValues(lambda x: x[0] + "," + x[1])
    joined_pairs = joined_pairs.sortByKey()
    rdd_final = joined_pairs.map(lambda x: x[0] + "," + x[1])
    rdd_final.saveAsTextFile("task1b.out")


