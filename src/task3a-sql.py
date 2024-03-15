from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
                print("Not enough arguments", file=sys.stderr)
                exit(-1)
        spark = SparkSession.builder.appName("task3a-sql").getOrCreate()
        df_taxi = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])
        df_invalid_fares = df_taxi.where(col("_c15") < 0).agg(count("*"))
        df_invalid_fares.select("*").write.option("quote", "").save('task3a-sql.out', format="csv")
    except Exception as e:
        print(e)
    

