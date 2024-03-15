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
        df_int = df_lic.select(col("_c20").alias("agent_name"), col("_c5").alias("fare_amount"))
        df_int_2 = df_int.groupBy("agent_name").agg(sum("fare_amount").alias("total_revenue"))
        df_int_3 = df_int_2.orderBy(col("total_revenue").desc(), col("agent_name").asc()).limit(10)
        df_int_4 = df_int_3.withColumn("total_revenue", format_number(df_int_3.total_revenue, 2))
        df_int_4.select("agent_name", "total_revenue").write.option("quote", "").save('task4c-sql.out', format="csv")
    except Exception as e:
        print(e)