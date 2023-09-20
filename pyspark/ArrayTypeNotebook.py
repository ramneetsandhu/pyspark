# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,ArrayType
from pyspark.sql.functions import col,array

# COMMAND ----------

data = [('abc', [1,2]), ('mno', [4,5]), ('xyz', [7,8])]
# schema = ['id', 'numbers']

schema = StructType([
    StructField('id', StringType()),
    StructField('numbers', dataType=ArrayType(IntegerType()))
])
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df2 = df.withColumn('firstNumber', col('numbers')[0])

# COMMAND ----------

df2.show()

# COMMAND ----------

data2 = [(1,2), (3,4)]
schema2 = ['num1', 'num2']
df3 = spark.createDataFrame(data=data2, schema=schema2)

# COMMAND ----------

df3.show()

# COMMAND ----------

df4 = df3.withColumn('numbers', array(col('num1'),col('num2')))

# COMMAND ----------

df4.show()

# COMMAND ----------

