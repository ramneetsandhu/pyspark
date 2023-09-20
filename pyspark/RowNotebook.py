# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

row = Row('rishi', 3000)
row1 = Row(name='singh', salary=5000)
row2 = Row(name='kesh', salary=10000)

# COMMAND ----------

row[0] + " " + str(row[1])

# COMMAND ----------

row1.name + " " + str(row1.salary)

# COMMAND ----------

data = [row2, row1]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

Person = Row('name', 'age')
person1 = Person('rishi', 30)
person2 = Person('kesh', 24)

# COMMAND ----------

personDf = spark.createDataFrame([person1, person2])
personDf.show()

# COMMAND ----------

data2 = [Row(name='rishi', prop=Row(age=30, gender='male')),
         Row(name='singh', prop=Row(age=40, gender='male'))]
df2 = spark.createDataFrame(data2)
df2.show()
df2.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

spark.sparkContext

# COMMAND ----------

