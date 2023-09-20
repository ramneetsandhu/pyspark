# Databricks notebook source
# advantage of spark, you can work with sql along with dataframes. That means, if you are comfortable with sql, you can create temporary view on dataframe bye using createorreplacetempview and use sql to select and manipluate data.
# temp views are session scoped and cannot be shared between the sessions

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data = [(1, 'rishi', 3000), (2, 'kesh', 2000), (3, 'singh', 4000)]
schema = ['id', 'name', 'salary']

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
df.show()

# COMMAND ----------

df.select('id', 'name').show()

# COMMAND ----------

help(df.createOrReplaceTempView)

# COMMAND ----------

df.createOrReplaceTempView('employees')

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, upper(name) as name from employees;

# COMMAND ----------

