# Databricks notebook source
from  pyspark.sql import *

# COMMAND ----------

help(DataFrameWriter)

# COMMAND ----------

help(dataframe)

# COMMAND ----------

data = [(1, 'rishi'), (2, 'kesh'), (3, 'singh')]
schema = ['id', 'name']

# COMMAND ----------

df = spark.createDataFrame(data=data, schema=schema)
display(df)

# COMMAND ----------

help(df.write)

# COMMAND ----------

df.write.csv(path='dbfs:/FileStore/tables/demo', header=True)

# COMMAND ----------

display(spark.read.csv(path='dbfs:/FileStore/tables/demo', header=True))

# COMMAND ----------

