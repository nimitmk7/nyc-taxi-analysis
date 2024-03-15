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
        spark = SparkSession.builder.appName("task3c-sql").getOrCreate()
        df_taxi = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])
        df_taxi_location = df_taxi.select(col("_c0").alias("medallion"), col("_c10").alias("pickup_longitude"), col("_c11").alias("pickup_latitude"), col("_c12").alias("dropoff_longitude"), col("_c13").alias("dropoff_latitude"))
        df_zero_mark = df_taxi_location.withColumn('isInvalid', F.when((F.col('pickup_longitude') == 0) & (F.col('pickup_latitude') == 0) & (F.col('dropoff_latitude') == 0) & (F.col('dropoff_longitude') == 0), 1).otherwise(0))
        df_intermediate = df_zero_mark.groupBy("medallion").agg(count("*").alias("total_trips"), sum("isInvalid").alias("invalid_trips"))
        df_result = df_intermediate.withColumn("percent_invalid_trips", format_number((df_intermediate.invalid_trips/df_intermediate.total_trips)*100, 2))
        df_result.select("medallion", "percent_invalid_trips").sort("medallion", ascending=True).write.option("quote", "").save('task3c-sql.out', format="csv")

    except Exception as e:
        print(e)

   