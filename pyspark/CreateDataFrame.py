# Databricks notebook source
dir(spark)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# data = [(1, 'rishi'), (2, 'kesh'), (3, 'singh')]
data = [{'id': 1, 'name': 'rishi'}, {'id': 2, 'name': 'kesh'}, {'id': 3, 'name': 'singh'}]
# schema = StructType([
#                         StructField(name='id', dataType=IntegerType()),
#                         StructField(name='name', dataType=StringType())
#                         ])
# df = spark.createDataFrame(data=data, schema=['id', 'name'])
# df = spark.createDataFrame(data=data, schema=schema)
df = spark.createDataFrame(data=data)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

