# Databricks notebook source
help(spark.read.json)

# COMMAND ----------

df = spark.read.json(path='dbfs:/FileStore/temp/demo.json')
display(df.show())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

## Multline json
df2 = spark.read.json(path='dbfs:/FileStore/temp/emp2.json', multiLine=True)
display(df2.show())

# COMMAND ----------

## Read Multiple Json Files
df3 = spark.read.json(path=['dbfs:/FileStore/temp/emp.json', 'dbfs:/FileStore/temp/demo.json'])
display(df3.show())

# COMMAND ----------

