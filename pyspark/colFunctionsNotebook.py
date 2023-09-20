# Databricks notebook source


# COMMAND ----------

data = [(1, 'rishi', 3000), (2, 'kesh', 4000), (3, 'singh', 5000)]
schema = ['id', 'name', 'salary']

# COMMAND ----------

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.select(df.id.alias('emp_id'), df.name, df.salary).show()

# COMMAND ----------

df.sort(df.name.asc()).show()

# COMMAND ----------

df.sort(df.name.desc()).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(df.id, df.name, df.salary.cast('int')).printSchema()

# COMMAND ----------

df.filter(df.name.like('r%')).show()

# COMMAND ----------

