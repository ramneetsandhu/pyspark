# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data = [
    (1, 'rishi', 'm', 5000, 'it'),
    (2, 'kesh', 'm', 6000, 'it'),
    (3, 'singh', 'f', 2500, 'payroll'),
    (4, 'aaa', 'm', 4000, 'hr'),
    (5, 'bbb', 'f', 2000, 'hr'),
    (6, 'ccc', 'f', 3000, 'payroll'),
    (7, 'ddd', 'm', 3000, 'it'),
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

from pyspark.sql.functions import count, min, max

# COMMAND ----------

df.groupBy('dep').agg(count('*').alias('CountOfEmps')).show()

# COMMAND ----------

df.groupBy('dep').agg(count('*').alias('CountOfEmps'),
                     min('salary').alias('minSalary'),
                     max('salary').alias('maxSalary')
                     ).show()


# COMMAND ----------

