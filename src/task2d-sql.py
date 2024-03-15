from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
                print("Not enough arguments", file=sys.stderr)
                exit(-1)
        spark = SparkSession.builder.appName("task2d-sql").getOrCreate()
        df_taxi = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])

        # Intermediate df with just medallion ID and date
        df_intermediate = df_taxi.select(col("_c0").alias("medallion"), to_date(col("_c3")).alias("date"))
        df_intermediate_1 = df_intermediate.groupBy("medallion", "date").agg(count("*").alias("day_trip_count")).sort("medallion")

        df_intermediate_2 = df_intermediate_1.groupBy("medallion").agg(count("*").alias("days_driven"), sum("day_trip_count").alias("total_trips")).sort("medallion", ascending=True)
        result_df = df_intermediate_2.withColumn('avg_trips_per_day', df_intermediate_2.total_trips/df_intermediate_2.days_driven)
        result_df.select(["medallion", "total_trips", "days_driven", "avg_trips_per_day"]).write.option("quote", "").save('task2d-sql.out', format="csv")
        
    except Exception as e: 
        print(e)    

    

