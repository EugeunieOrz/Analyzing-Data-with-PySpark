from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import sys
import pandas as pd

sc = SparkSession \
    .builder \
    .appName("temperatures") \
    .getOrCreate()

# Please enter a set of cities as the command-line arguments
cities = sys.argv # for example, Kyiv Paris

data_file = "C:/Users/eugen/Documents/temperatures/temperatures.csv"
df = sc.read.csv(data_file, header=True, sep=",").cache()
df2 = df.withColumn('Temperature', df['Temperature'].cast('double'))
dfByMonth = df2.withColumn('Date', F.date_format('Date', 'YYYY-MM'))

# 4 Max, Min, Avg Temperature per month for a given N of cities
df_cities = dfByMonth.filter(dfByMonth.City.isin(cities))

df_result = df_cities.groupBy("City", "Date").agg(
    max(col("Temperature")).alias("MaxTemp"),
    min(col("Temperature")).alias("MinTem"),
    round(mean(col("Temperature")), 1).alias("AvgTemp"))

df_result.toPandas().to_csv("C:/Users/eugen/Documents/temperatures/task-4.csv")
