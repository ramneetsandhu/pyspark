# Databricks notebook source
# MAGIC %scala
# MAGIC spark

# COMMAND ----------

data = [(1, 'rishi'), (2, 'kesh'), (3, 'singh')]
schema = ['id', 'name']

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
df.show()

# COMMAND ----------

df.createOrReplaceGlobalTempView('emp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.emp

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables('default')

# COMMAND ----------

spark.catalog.listTables('global_temp')

# COMMAND ----------

