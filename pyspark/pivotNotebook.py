# Databricks notebook source
# Pivot 
# its used to rotate data in one column into multiple columns.
# its an aggregration where one of the grouping column values will be converted in individual columns.

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data = [
    (1, 'rishi', 'male', 'IT'),
    (2, 'kesh', 'male', 'IT'),
    (3, 'singh', 'female', 'IT'),
    (4, 'annu', 'female', 'HR'),
    (5, 'shakti', 'female', 'IT'),
    (6, 'pradeep', 'male', 'HR'),
    (7, 'sarfaraj', 'male', 'HR'),
    (8, 'aisha', 'female', 'IT'),
]

schema = ['id', 'name', 'gender', 'dep']

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
df.show()

# COMMAND ----------

df.groupBy('dep', 'gender').count().show()

# COMMAND ----------

# pivot
df.groupBy('dep').pivot('gender').count().show()

# COMMAND ----------

df.groupBy('dep').pivot('gender', ['male', 'female']).count().show()

# COMMAND ----------

