# Databricks notebook source


# COMMAND ----------

data = [(1, 'rishi', 'male', 2000), (2, 'kesh', 'male', 3000), (2, 'kesh', 'male', 3000),(3, 'aaa', 'female', 3500)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.show()

# COMMAND ----------

help(df.distinct)

# COMMAND ----------

df.distinct().show()

# COMMAND ----------

help(df.dropDuplicates)

# COMMAND ----------

df.dropDuplicates().show()

# COMMAND ----------

df.dropDuplicates(['gender']).show()

# COMMAND ----------

