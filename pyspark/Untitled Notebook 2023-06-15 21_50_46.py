# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data1 = [(1, 'rishi', 30)]
schema1 = ['id', 'name', 'age']


data2 = [(1, 'kesh', 2000)]
schema2 = ['id', 'name', 'salary']

# COMMAND ----------

df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)

df1.show()
df2.show()

# COMMAND ----------

df1.unionByName(df2, allowMissingColumns=True).show()

# COMMAND ----------

