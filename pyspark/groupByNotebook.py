# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data = [(1, 'rishi', 'm', 5000, 'it'), 
        (2, 'kesh', 'm', 6000, 'it'),
        (3, 'singh', 'm', 7000, 'payroll'),
        (4, 'aaa', 'f', 4000, 'hr'),
        (5, 'sss', 'f', 6000, 'hr'),
        (6, 'ppp', 'f', 3000, 'payroll')
       ]
schema = ['id', 'name', 'gender', 'salary', 'dep']

# COMMAND ----------

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.groupBy('dep').count().show()

# COMMAND ----------

df.groupBy('dep').min('salary').show()

# COMMAND ----------

