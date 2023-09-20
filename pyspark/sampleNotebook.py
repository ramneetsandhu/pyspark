# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df = spark.range(start=1, end=101)
df.show()

# COMMAND ----------

df1 = df.sample(fraction=0.1) # 0.1 <= 10% of entire dataset
df1.show()

# COMMAND ----------

df2 = df.sample(fraction=0.2)
df2.show()

# COMMAND ----------

df3 = df.sample(fraction=0.1, seed=123) # seed value will help to fixed value everytime
df3.show()

# COMMAND ----------

