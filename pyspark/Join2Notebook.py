# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data1 = [(1, 'rishi', 2000, 2), (2, 'kesh', 3000, 1), (3, 'singh', 1000, 4)]
schema1 = ['id', 'name', 'salary', 'dep']

data2 = [(1, 'IT'), (2, 'HR'), (3, 'Payroll')]
schema2 = ['id', 'name']

# COMMAND ----------

empDf = spark.createDataFrame(data1, schema=schema1)
depDf = spark.createDataFrame(data2, schema=schema2)

empDf.show()

# COMMAND ----------

empDf.join(depDf, empDf.dep == depDf.id, 'inner').show()

# COMMAND ----------

# show only  column from left table
empDf.join(depDf, empDf.dep == depDf.id, 'leftsemi').show()

# COMMAND ----------

# show only  not matching row
empDf.join(depDf, empDf.dep == depDf.id, 'leftanti').show()

# COMMAND ----------

# self join
data3 = [(1, 'rishi', 0), (2, 'kesh', 1), (3, 'singh', 2)]
schema3 = ['id', 'name', 'managerId']

df = spark.createDataFrame(data3, schema3)
df.show()

from pyspark.sql.functions import col
df.alias('empData').join(df.alias('mgrData'), col('empData.ManagerId') == col('mgrData.id') , 'inner')\
    .select(col('empData.name').alias('empName'), col('mgrData.name').alias('mgrName')).show()

# COMMAND ----------

