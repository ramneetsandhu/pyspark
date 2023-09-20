# Databricks notebook source
data = [(1, 'sdfdf'), (2, 'dsf'), (3, 'dfsrewr'), (4, 'qeqe')]
schema = ['id', 'comments']

# COMMAND ----------

df = spark.createDataFrame(data=data, schema=schema)

# COMMAND ----------

df.show()

# COMMAND ----------

help(df.show)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df.show(n=2)

# COMMAND ----------

df.show(vertical=True)

# COMMAND ----------

