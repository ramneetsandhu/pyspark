# Databricks notebook source
from pyspark.sql.functions import upper

# COMMAND ----------

data = [(1, 'rishi', 2000), (2, 'kesh', 3000), (3, 'singh', 4000)]
schema = ['id', 'name', 'salary']

# COMMAND ----------

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

help(df.transform)

# COMMAND ----------

df.withColumn('name', upper(df.name)).show()

# COMMAND ----------

def convertNameToUpper(df):
    return df.withColumn('name', upper(df.name))

def doubleTheSalary(df):
    return df.withColumn('Salary', df.salary * 2)

# COMMAND ----------

df1 = df.transform(convertNameToUpper)

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = df1.transform(doubleTheSalary)
df2.show()

# COMMAND ----------

