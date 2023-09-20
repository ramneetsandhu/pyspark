# Databricks notebook source
from pyspark.sql.functions import transform, upper

# COMMAND ----------

help(transform)

# COMMAND ----------

data = [(1, 'rishi', ['azure', 'dotnet']), (2, 'kesh', ['aws', 'java'])]
schema = ['id', 'name', 'skills']

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
df.show()

df.printSchema()

# COMMAND ----------

df.select('id', 'name', transform('skills', lambda x: upper(x)).alias('skills')).show()

# COMMAND ----------

def convertArrayToUpper(x):
    return upper(x)

# COMMAND ----------

df.select('id', 'name', transform('skills', convertArrayToUpper ).alias('skills')).show()

# COMMAND ----------

