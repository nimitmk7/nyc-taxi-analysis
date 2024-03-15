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
        spark = SparkSession.builder.appName("task3d-sql").getOrCreate()
        df_taxi = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])
        df_licenses = df_taxi.select(col("_c0").alias("medallion"), col("_c1").alias("hack_license"))
        df_result = df_licenses.groupBy("hack_license").agg(countDistinct("medallion").alias("distinct_medallion"))
        df_result.select("hack_license", "distinct_medallion").sort("hack_license", ascending = True).write.option("quote", "").save('task3d-sql.out', format = 'csv')
    except Exception as e:
        print(e)