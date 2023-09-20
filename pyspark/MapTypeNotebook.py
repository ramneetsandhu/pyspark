# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import explode, map_keys, map_values

# COMMAND ----------

data = [('rishi', {'hair':'black', 'eye': 'black'}), ('kesh', {'hair': 'grey', 'eye': 'blue'})]
# schema = ['name', 'properties']
schema = StructType([
        StructField('name', StringType()),
        StructField('properties', MapType(StringType(), StringType()))
])
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.show(truncate=False)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df2 = df.withColumn('hair', df.properties['hair'])
df2.show()

# COMMAND ----------

df3 = df.withColumn('eye', df.properties.getItem('eye'))
df3.show()

# COMMAND ----------

df4 = df.select('name', 'properties', explode(df.properties))

# COMMAND ----------

df4.show()

# COMMAND ----------

df5 = df.withColumn('keys', map_keys(df.properties))
df5.show()

# COMMAND ----------

df6 = df.withColumn('values', map_values(df.properties))
df6.show()

# COMMAND ----------

