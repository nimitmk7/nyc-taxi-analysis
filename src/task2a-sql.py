from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from csv import reader

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            print("Not enough arguments", file=sys.stderr)
            exit(-1)
        
        spark = SparkSession.builder.appName("task2a-sql").getOrCreate()
        df_taxi = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(sys.argv[1])

        ranges = [[0,5],[5,15],[15,30],[30,50],[50,100], [100, '>100']]
        dataList = []
        
        # Column _c15 is fare amount variable
        for range in ranges:
            lower_bound = range[0]
            higher_bound = range[1]
            if lower_bound > 0 and lower_bound <100:
                range_count = df_taxi.filter((df_taxi._c15 > lower_bound) & (df_taxi._c15 <= higher_bound)).count()
                dataList.append(dict({"range": str(lower_bound) + "," + str(higher_bound), "amount": range_count}))
            elif lower_bound == 0:
                range_count = df_taxi.filter((df_taxi._c15 >= lower_bound) & (df_taxi._c15 <= higher_bound)).count()
                dataList.append(dict({"range": str(lower_bound) + "," + str(higher_bound), "amount": range_count}))
            else:
                range_count = df_taxi.filter(df_taxi._c15 >= lower_bound).count()
                dataList.append(dict({"range": str(lower_bound) + "," + str(higher_bound), "amount": range_count}))
        result_df = spark.createDataFrame(dataList) 
        result_df.select(["range", "amount"]).sort("amount", ascending= True).write.option("quote", "").save('task2a-sql.out', format="csv")         
    except Exception as e:
        print(e)