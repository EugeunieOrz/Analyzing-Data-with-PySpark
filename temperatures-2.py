from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import sys
import pandas as pd

sc = SparkSession \
    .builder \
    .appName("temperatures") \
    .getOrCreate()

# Please enter a month number and the corresponding number of records
month = sys.argv[1] # for example, 10
num_of_records = sys.argv[2] # for example, 8 records

data_file = "C:/Users/eugen/Documents/temperatures/temperatures.csv"
df = sc.read.csv(data_file, header=True, sep=",").cache()
df2 = df.withColumn('Temperature', df['Temperature'].cast('double'))
df3 = df2.withColumn('Date', F.date_format('Date', 'YYYY-MM'))

max_temp_name = "Max Temp for all months, if " + month + " month has at least " + num_of_records + " records"
min_temp_name = "Min Temp for all months, if " + month + " month has at least " + num_of_records + " records"
avg_temp_name = "Avg Temp for all months, if " + month + " month has at least " + num_of_records + " records"

new_column_names1 = ["City", "Date", max_temp_name]
new_column_names2 = ["City", "Date", min_temp_name]
new_column_names3 = ["City", "Date", avg_temp_name]

# 2 Max, Min, Avg Temperature per month for each city, if a given month has at least given number of records

# Max Temperature
max_temp_df = df3.groupBy("City", "Date").agg(when(
    df3.Date.contains(month) & (count("*") >= int(num_of_records)), F.max(col("Temperature"))).when(
    ~df3.Date.contains(month), F.max(col("Temperature")))).toDF(*new_column_names1)

# Min Temperature
min_temp_df = df3.groupBy("City", "Date").agg(when(
    df3.Date.contains(month) & (count("*") >= int(num_of_records)), F.min(col("Temperature"))).when(
    ~df3.Date.contains(month), F.min(col("Temperature")))).toDF(*new_column_names2)

# Avg Temperature
avg_temp_df = df3.groupBy("City", "Date").agg(when(
    df3.Date.contains(month) & (count("*") >= int(num_of_records)), round(F.mean(col("Temperature")), 1)).when(
    ~df3.Date.contains(month), round(F.mean(col("Temperature")), 1))).toDF(*new_column_names3)

df4 = max_temp_df.join(min_temp_df, ["City", "Date"])
df_result = df4.join(avg_temp_df, ["City", "Date"])

df_result.toPandas().to_csv("C:/Users/eugen/Documents/temperatures/task-2.csv")
