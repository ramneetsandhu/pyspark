# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data = [(1, 'rishi', 'male', 2000)]
schema = ['id', 'name', 'gender', 'salary']

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
df.show()

# COMMAND ----------

df.select('name', 'gender').show()

# COMMAND ----------

df.select(['id', 'name']).show()

# COMMAND ----------

df.select(df.id, df.name).show()

# COMMAND ----------

df.select(df['id'], df['gender']).show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.select(col('name'), col('salary')).show()

# COMMAND ----------

df.select([col for col in df.columns]).show()

# COMMAND ----------

df.select("*").show()

# COMMAND ----------

