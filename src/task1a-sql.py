from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from csv import reader
from pyspark.sql.functions import when

if __name__ == "__main__":
    try:
        if len(sys.argv) != 3:
            print("Not enough arguments", file=sys.stderr)
            exit(-1)

        spark = SparkSession.builder.appName("task1a-sql").getOrCreate()
        df_trips = spark.read.format('csv').options(header = 'true', inferSchema = 'true').load(sys.argv[1])
        df_fares = spark.read.format('csv').options(header = 'true', inferSchema = 'true').load(sys.argv[2])

        key_list = ["medallion", "hack_license", "vendor_id", "pickup_datetime"]
        sort_key_list = ["medallion", "hack_license", "pickup_datetime"]
        combined_df = df_trips.join(df_fares, key_list, "inner")

        attributes_ordered_list = ["medallion", "hack_license", "vendor_id", "pickup_datetime", "rate_code", "store_and_fwd_flag", "dropoff_datetime", "passenger_count", 
                                   "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", 
                                    "dropoff_latitude", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", 
                                    "tolls_amount", "total_amount"]

        combined_df.select(attributes_ordered_list).sort(sort_key_list, ascending=True).write.save('task1a-sql.out', format="csv", nullValue = "null")
        spark.stop()
    except Exception as e:
        print(e)







