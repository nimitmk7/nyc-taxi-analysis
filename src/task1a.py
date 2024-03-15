from __future__ import print_function

import sys
from pyspark import SparkContext

def construct_trip_pairs(list):
    key = list[0:3]
    key.append(list[5])
    value = list[3:5] + list[6:]
    key = ",".join(key)
    value = ",".join(value)
    return (key, value)

def construct_fare_pairs(list):
    key = list[0:4]
    value = list[4:]
    key = ",".join(key)
    value = ",".join(value)
    return (key, value)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Not enough arguments", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    rdd_trips = sc.textFile(sys.argv[1], 4)
    rdd_fares = sc.textFile(sys.argv[2], 4)

    # Transforming RDD into list of lists, where each list is a row in the file
    trips_list = rdd_trips.map(lambda x: x.split(',')).filter(lambda x: ('medallion' not in x))
    fares_list = rdd_fares.map(lambda x: x.split(',')).filter(lambda x: ('medallion' not in x))

    trip_pairs = trips_list.map(lambda x: construct_trip_pairs(x))
    fare_pairs = fares_list.map(lambda x: construct_fare_pairs(x))

    joined_pairs = trip_pairs.join(fare_pairs).mapValues(lambda x: x[0] + "," + x[1])
    joined_pairs = joined_pairs.sortByKey()
    rdd_final = joined_pairs.map(lambda x: x[0] + "," + x[1])
    rdd_final.saveAsTextFile("task1a.out")


