# Databricks notebook source
my_data=[
(1,4),
(2,2), 
(3,5),
(3,3),
(5,3)]

# COMMAND ----------

my_schema = ['id'  , 'number']

# COMMAND ----------

spark.createDataFrame(data=my_data,schema=my_schema).show()

# COMMAND ----------

