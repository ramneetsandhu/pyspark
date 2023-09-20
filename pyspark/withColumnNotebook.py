# Databricks notebook source
data = [(1, 'Rishi', '3000'), (2, 'kesh', '4000'), (3, 'singh', '5000')]
columns = ['id', 'name', 'salary']

# COMMAND ----------

df = spark.createDataFrame(data=data, schema=columns)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df1 = df.withColumn(colName='salary', col=col('Salary').cast('Integer'))

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2 = df.withColumn(colName='salary', col=col('Salary') * 2)

# COMMAND ----------

df2.show()

# COMMAND ----------

from pyspark.sql.functions import lit
df3 = df.withColumn(colName='country',  col=lit('IND'))

# COMMAND ----------

df3.show()

# COMMAND ----------

df4 = df.withColumn('copiedSalary', col('salary'))

# COMMAND ----------

df4.show()

# COMMAND ----------

