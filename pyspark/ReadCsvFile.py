# Databricks notebook source
help(spark.read)

# COMMAND ----------

df = spark.read.csv(path='dbfs:/FileStore/tables/employment_data.csv', header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

### Second way to read csv file ###

# df2 = spark.read.format('csv').load('dbfs:/FileStore/tables/employment_data.csv', header=True)
df2 = spark.read.format('csv').option(key='header', value=True).load('dbfs:/FileStore/tables/employment_data.csv')

# COMMAND ----------

display(df2)

# COMMAND ----------

