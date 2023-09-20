# Databricks notebook source
data = [(1, 'Rishi', '3000'), (2, 'kesh', '4000'), (3, 'singh', '5000')]
columns = ['id', 'name', 'salary']

# COMMAND ----------

df = spark.createDataFrame(data=data, schema=columns)

# COMMAND ----------

display(df)

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df2 = df.withColumnRenamed('salary', 'salary_amount')

# COMMAND ----------

df2.show()

# COMMAND ----------

