from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from csv import reader

if __name__ == "__main__":
    try:
        if len(sys.argv) != 3:
            print("Not enough arguments", file=sys.stderr)
            exit(-1)

        spark = SparkSession.builder.appName("task1b-sql").getOrCreate()
        df_fares = spark.read.format('csv').options(header = 'true', inferSchema = 'true').load(sys.argv[1])
        df_licenses = spark.read.format('csv').options(header = 'true', inferSchema = 'true').load(sys.argv[2])

        key = "medallion"
        combined_df = df_fares.join(df_licenses, key, "inner")

        attributes_ordered_list = ["medallion", "hack_license",  "vendor_id", "pickup_datetime", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", 
                            "tolls_amount", "total_amount", "name", "type", "current_status", 
                            "DMV_license_plate", "vehicle_VIN_number", "vehicle_type", "model_year", "medallion_type", 
                            "agent_number", "agent_name", "agent_telephone_number","agent_website", "agent_address", "last_updated_date", 
                            "last_updated_time"]
        
        combined_df.select(attributes_ordered_list).sort(key, ascending=True).write.save('task1b-sql.out', format="csv")
        spark.stop()
    except Exception as e:
        print(e)







