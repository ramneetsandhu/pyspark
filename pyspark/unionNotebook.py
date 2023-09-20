# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data = [(1, 'rishi', 'male'), (2, 'kesh', 'male')]
schema = ['id', 'name', 'age']

# COMMAND ----------

data2 = [(2, 'kesh', 'male'),(3, 'aaa', 'female'), (4, 'bbb', 'female')]
schema2 = ['id', 'name', 'age']

# COMMAND ----------

df = spark.createDataFrame(data, schema)
df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df3 = df.union(df2)
df3.show()

# COMMAND ----------

df4 = df.unionAll(df2)
df4.show()

# COMMAND ----------

