# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

data = [(1, 'rishi', 3000), (2, 'kesh', 4000), (3, 'singh', 5000)]
# schema = ['id', 'name', 'salary']
schema = StructType([
        StructField(name='id', dataType=IntegerType()),
        StructField(name='name', dataType=StringType()),
        StructField(name='salary', dataType=IntegerType())
])

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

## Nested Columns ##
data2 = [(1, ('sukh', 'dev'), 3000), (2, ('john', 'dave'), 4000), (3, ('rishi','singh'), 5000)]

StructName = StructType([
                    StructField(name='firstname', dataType=StringType()),
                    StructField('lastname', StringType())
])
schema2 = StructType([
        StructField(name='id', dataType=IntegerType()),
        StructField(name='name', dataType=StructName),
        StructField(name='salary', dataType=IntegerType())
])

# COMMAND ----------

df2 = spark.createDataFrame(data2, schema=schema2)

# COMMAND ----------

df2.show()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

