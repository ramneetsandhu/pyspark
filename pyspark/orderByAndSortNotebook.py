# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("OrderByAndSort App")
spark = spark.getOrCreate()

# COMMAND ----------

data = [(1, 'rishi', 'm', 2000, 'it'), (2, 'kesh', 'm', 3000, 'hr'), (3, 'singh', 'f', 4000, 'payroll'), (4, 'aaa', 'f', 3000, 'hr')]

schema = ['id', 'name', 'gender', 'salary', 'dep']

df = spark.createDataFrame(data=data, schema=schema)

# COMMAND ----------

df.show()

# COMMAND ----------

dir(spark)

# COMMAND ----------

spark.getActiveSession()

# COMMAND ----------

spark.sparkContext.getConf().getAll()

# COMMAND ----------

help(df.sort)

# COMMAND ----------

df.sort('dep').show()

# COMMAND ----------

df.sort(['dep', 'id']).show()

# COMMAND ----------

df.orderBy(df.dep).show()

# COMMAND ----------

df.orderBy(df.dep, df.id).show()

# COMMAND ----------

df.sort(df.dep.desc(), df.id).show()

# COMMAND ----------

