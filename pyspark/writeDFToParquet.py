# Databricks notebook source
data = [(1, 'Rishi'), (2, 'Kesh'), (3, 'Singh')]
schema = ['id', 'name']

# COMMAND ----------

df = spark.createDataFrame(data=data, schema=schema)
display(df)

# COMMAND ----------

help(df.write.parquet)

# COMMAND ----------

df.write.parquet(path='dbfs:/FileStore/parqueOutput')

# COMMAND ----------

df1 = spark.read.parquet('dbfs:/FileStore/parqueOutput')
display(df1)

# COMMAND ----------

# df.write.parquet(path='dbfs:/FileStore/parqueOutput', mode='error')
# df.write.parquet(path='dbfs:/FileStore/parqueOutput', mode='ignore')
# df.write.parquet(path='dbfs:/FileStore/parqueOutput', mode='overwrite')
df.write.parquet(path='dbfs:/FileStore/parqueOutput', mode='append')

# COMMAND ----------

df2 = spark.read.parquet('dbfs:/FileStore/parqueOutput')
display(df2)

# COMMAND ----------

