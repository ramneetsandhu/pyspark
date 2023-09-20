# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data = [(1, 'rishi', 'male', 1000, None), (2, 'kesh', 'female', 2000, 'IT'), (3, 'abcd', None, 1000, 'HR')]

schema = ['id', 'name', 'gender', 'salary', 'dep']


# COMMAND ----------

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

help(df.fillna)

# COMMAND ----------

df.fillna('unknown').show()

# COMMAND ----------

df.fillna('unknown', 'gender').show()

# COMMAND ----------

df.fillna('unknown', ['gender', 'dep']).show()

# COMMAND ----------

df.na.fill('none', ['gender']).show()

# COMMAND ----------

df.na.fill('NA', ['gender', 'dep']).show()

# COMMAND ----------

df.fillna({'gender':'Other' , 'dep':'staff'}).show()

# COMMAND ----------

