# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data1 = [(1, 'rishi', 2000, 2), (2, 'kesh', 3000, 1), (3, 'singh', 1000,4)]
schema1 = ['id', 'name', 'salary', 'dep']

data2 = [(1, 'it'), (2, 'hr'), (3, 'payroll')]
schema2 = ['id', 'name']

empDf = spark.createDataFrame(data1, schema1)
depDf = spark.createDataFrame(data2, schema2)

empDf.show()
depDf.show()

# COMMAND ----------

empDf.join(depDf, empDf.dep==depDf.id, 'inner').show()

# COMMAND ----------

empDf.join(depDf, empDf.dep==depDf.id, 'left').show()

# COMMAND ----------

empDf.join(depDf, empDf.dep==depDf.id, 'right').show()

# COMMAND ----------

empDf.join(depDf, empDf.dep==depDf.id, 'full').show()

# COMMAND ----------

empDf.join(depDf, empDf.dep==depDf.id, 'outer').show()

# COMMAND ----------

