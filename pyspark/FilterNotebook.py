# Databricks notebook source


# COMMAND ----------

data = [(1, 'rishi', 'male', 3200), (2, 'kesh', 'female', 4000), (3, 'singh', 'other', 5000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.filter(df.gender == 'male').show()

# COMMAND ----------

df.filter("gender == 'male'").show()

# COMMAND ----------

df.where(df.gender == 'female').show()

# COMMAND ----------

df.where("gender == 'other'").show()

# COMMAND ----------

df.where((df.gender == 'other') & (df.salary > 1000)).show()

# COMMAND ----------

