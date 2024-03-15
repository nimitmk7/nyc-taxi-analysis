from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
                print("Not enough arguments", file=sys.stderr)
                exit(-1)
        spark = SparkSession.builder.appName("task2c-sql").getOrCreate()
        df_taxi = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])

        # _c3 is pickup_datetime
        df_taxi_transformed = df_taxi.withColumn("date", to_date("_c3")).withColumn("total_revenue", col("_c16") + col("_c15") + col("_c18"))
        result_df = df_taxi_transformed.groupBy("date").agg(round(sum("total_revenue"), 2).alias("total_revenue"), round(sum("_c19"), 2).alias("total_tolls"))
        result_df.sort("date", ascending= True).write.option("quote", "").save('task2c-sql.out', format="csv")

    except Exception as e: 
        print(e)
    




