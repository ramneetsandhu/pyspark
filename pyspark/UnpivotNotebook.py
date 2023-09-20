# Databricks notebook source
# unpivot is rotating columns into rows. 
# PySpark SQL does not have unpivot function hence will use the stack function

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

data = [('IT', 8, 5), ('Payroll', 3, 2), ('HR', 2, 4)]
schema = ['dep', 'male', 'female']

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
df.show()

# COMMAND ----------

df.select('dep').show()

# COMMAND ----------

unpivotDf = df.select('dep', expr("stack(2, 'male', male, 'female', female) as (gender, count)"))
unpivotDf.show()

# COMMAND ----------

