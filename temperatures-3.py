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
df2 = df.withColumn('Temperature', df['Temperature'].cast('double'))
dfByMonth = df2.withColumn('Date', F.date_format('Date', 'YYYY-MM'))

# 3 Difference between Temperature per day and Min Temperature per month,
# difference between Temperature per day and Max Temperature per month,
# difference between Temperature per day and Avg Temperature per month.

df_max_min_avg = dfByMonth.groupBy("City", "Date").agg(
    max(col("Temperature")).alias("MaxTemp"),
    min(col("Temperature")).alias("MinTemp"),
    round(F.mean(col("Temperature")), 1).alias("AvgTemp"))

max_min_avg = df_max_min_avg.selectExpr(
    "City as City", "Date as Month", "MaxTemp as MaxTemp", "MinTemp as MinTemp", "AvgTemp")

df_joined = max_min_avg.alias("a").join(
    df2.alias("b"), (df2.City == max_min_avg.City) & df2.Date.contains(max_min_avg.Month)).select(
    "b.City", "b.Date", "b.Temperature", "a.MaxTemp", "a.MinTemp", "a.AvgTemp")
df_joined_1 = df_joined.withColumn("Temp - MaxTemp", round(df_joined.Temperature - df_joined.MaxTemp, 1))
df_joined_2 = df_joined_1.withColumn("Temp - MinTemp", round(df_joined.Temperature - df_joined.MinTemp, 1))
df_result = df_joined_2.withColumn("Temp - AvgTemp", round(df_joined.Temperature - df_joined.AvgTemp, 1))

df_result.toPandas().to_csv("C:/Users/eugen/Documents/temperatures/task-3.csv")
