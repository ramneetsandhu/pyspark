# Databricks notebook source
data = [(1, 'rishi'), (2, 'kesh'), (3, 'singh')]
schema = ['id', 'name']

# COMMAND ----------

df = spark.createDataFrame(data=data, schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

help(df.write.json)

# COMMAND ----------

df.write.json(path='dbfs:/FileStore/emp')

# COMMAND ----------

display(spark.read.json(path='dbfs:/FileStore/emp'))

# COMMAND ----------

