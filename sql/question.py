# Databricks notebook source
from pyspark.sql import SparkSession
from os.path import abspath

# COMMAND ----------

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# COMMAND ----------

col1 = ['PersonId', 'FirstName', 'LastName']
col2 = ['AddressId', 'PersonId', 'City', 'State']

# COMMAND ----------

data1 = [(1, 'rishi', 'singh'), (2, 'dave', 'smith')]
data2 = [(1, 1, 'delhi', 'India'), (2,2, 'New york', 'USA')]

# COMMAND ----------

df1 = spark.createDataFrame(data1, col1).write.mode('overwrite').saveAsTable('person')
df2 = spark.createDataFrame(data2, col2).write.mode('overwrite').saveAsTable('address')

# COMMAND ----------

df1 = spark.read.table("person")
df2 = spark.read.table("address")

df1.show()
df2.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select FirstName, LastName, City, State
# MAGIC from person as p left join address as a on p.PersonId = a.PersonId

# COMMAND ----------

