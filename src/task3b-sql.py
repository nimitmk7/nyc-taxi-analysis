from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
                print("Not enough arguments", file=sys.stderr)
                exit(-1)
        spark = SparkSession.builder.appName("task3b-sql").getOrCreate()
        df_taxi = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])
        df_taxi_new = df_taxi.select(col("_c0").alias("medallion"), col("_c3").alias("pickup_datetime"))
        df_taxi_duplicates = df_taxi_new.select('medallion', 'pickup_datetime').groupBy('medallion', 'pickup_datetime').count().where(F.col('count') > 1)
        df_taxi_duplicates.select("medallion", "pickup_datetime").sort("medallion", ascending = True).write.option("quote", "").save('task3b-sql.out', format="csv")
    except Exception as e:
        print(e)