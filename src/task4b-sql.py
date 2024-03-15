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
        spark = SparkSession.builder.appName("task4a-sql").getOrCreate()
        df_lic = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])
        # medallion_type -> c18, total_revenue -> fare_amount(c5), tip -> c8, 
        df_int = df_lic.select(col("_c18").alias("medallion_type"), col("_c5").alias("fare_amount"), col("_c8").alias("tip"))
        df_int_2 = df_int.withColumn("tip_percentage", format_number((df_int.tip/df_int.fare_amount) * 100, 2))
        df_result = df_int_2.groupBy("medallion_type").agg(count("*").alias("tot_trip_count"), format_number(sum(col("fare_amount")), 2).alias("total_revenue"), format_number(avg("tip_percentage"),2).alias("avg_tip_percentage"))
        df_result.select("medallion_type", "tot_trip_count", "total_revenue", "avg_tip_percentage").sort("medallion_type", ascending=True).write.option("quote", "").save('task4b-sql.out', format="csv")
        

    except Exception as e:
        print(e)