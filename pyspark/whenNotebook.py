# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, otherwise

# COMMAND ----------

spar = SparkSession.builder.appName('Spark').getOrCreate()

# COMMAND ----------

data = [(1, 'rishi', 'M', 2000), (2, 'emma', 'F', 3000), (3, 'singh', '', 4000)]
schema = ['id', 'name', 'gender', 'salary']

# COMMAND ----------

df = spar.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df1 = df.select(df.id, df.name, when(df['gender']=='M', 'male')
                .when(df.gender=='F', 'female').otherwise('unknown').alias('Gender'))
df1.show()

# COMMAND ----------

