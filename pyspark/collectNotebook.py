# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# collect() retreives all elements in a dataframe as an array of row type to the driver node.
# collect() use it with small

# COMMAND ----------

data = [(1, 'rishi', '2000'), (2, 'kesh', 3000)]
schema = ['id', 'name', 'salary']


# COMMAND ----------

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

help(df.collect)

# COMMAND ----------

listrows = df.collect()
print(listrows)

# COMMAND ----------

listrows

# COMMAND ----------

listrows[0]

# COMMAND ----------

print(listrows[1]['name'])
print(listrows[0][1])

# COMMAND ----------

