from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import pandas as pd


sc = SparkSession \
    .builder \
    .appName("temperatures") \
    .getOrCreate()
data_file = "C:/Users/eugen/Documents/temperatures/temperatures.csv"
df = sc.read.csv(data_file, header=True, sep=",").cache()
dfToDouble = df.withColumn('Temperature', df['Temperature'].cast('double'))
dfByMonth = dfToDouble.withColumn('Date', F.date_format('Date', 'YYYY-MM'))

# 1 Max, Min, Avg Temperature per month for each city
df_result = dfByMonth.groupBy("City", "Date").agg(
    max(col("Temperature")).alias("Max Temp"),
    min(col("Temperature")).alias("Min Temp"),
    round(mean(col("Temperature")), 1).alias("Avg Temp"))

df_result.toPandas().to_csv("C:/Users/eugen/Documents/temperatures/task-1.csv")
