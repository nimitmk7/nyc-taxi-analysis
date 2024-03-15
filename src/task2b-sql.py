from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from csv import reader
from pyspark.sql.functions import col

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            print("Not enough arguments", file=sys.stderr)
            exit(-1)
        spark = SparkSession.builder.appName("task2b-sql").getOrCreate()
        df_taxi = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])

        # Column c7 is no of passengers
        result_df = df_taxi.groupBy(df_taxi._c7.alias("no_of_passengers")).agg(count("*").alias("trip_count"))
        result_df.sort("no_of_passengers", ascending= True).write.option("quote", "").save('task2b-sql.out', format="csv")         

    except Exception as e:
        print(e)