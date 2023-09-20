# Databricks notebook source
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# COMMAND ----------

col1 = lit('abcd')

# COMMAND ----------

type(col1)

# COMMAND ----------

data = [('rishi', 'male', 2000), ('kesh', 'male', 3000)]
schema = ['name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1 = df.withColumn('newCol', lit('newColVal'))
df1.show()
df1.printSchema()

# COMMAND ----------

df1.select(df1.name).show()

# COMMAND ----------

df1.select(col('name')).show()

# COMMAND ----------

data2 = [('rishi', 'male', 2000, ('black', 'brown')), ('kesh', 'male', 4000, ('red', 'blue'))]
propsType = StructType([
    StructField('hair', StringType()),
    StructField('eye', StringType())
])
schema2 = StructType([
    StructField('name', StringType()),
    StructField('gender', StringType()),
    StructField('salary', IntegerType()),
    StructField('props', propsType)
])

df2 = spark.createDataFrame(data2, schema2)
df2.show()
df2.printSchema()

# COMMAND ----------

df2.select(df2['props.eye']).show()

# COMMAND ----------

