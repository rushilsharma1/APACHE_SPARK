# Databricks notebook source
# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header", "false")\
        .option("inferschema","false")\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/2010_summary-3.csv")
flight_df.show(5)    

# COMMAND ----------

flight_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC NULLABLE = TRUE K MTLB HAI KI NULL VALUE HAI KI NHI 
# MAGIC

# COMMAND ----------

flight_df.columns

# COMMAND ----------

flight_df.select('_c0').show()

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

flight_df.select(col('_c0')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC explicitally usse column ko choose krenge.

# COMMAND ----------

flight_df.select('_c0 +5').show()

# COMMAND ----------

flight_df.select(col('_c2 ') +5 ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC TO SELECT MULTIPLE COLUMN

# COMMAND ----------

flight_df.select(col('_c0'),col('_c1')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC EXPRESSION 

# COMMAND ----------

flight_df.select(expr("_c0 as id"),expr("_c1 as name"),expr("concat(id,name)")).show()

# COMMAND ----------

flight_df.select('*').show()

# COMMAND ----------

# MAGIC %md
# MAGIC SPARK SQL

# COMMAND ----------

# MAGIC %md
# MAGIC phele ham apne table ko ek temporary view type ka bana lenge

# COMMAND ----------

flight_df.createOrReplaceTempView('flight_view')


# COMMAND ----------

spark.sql("""
          
          select * from flight_view 
          """).show()

# COMMAND ----------

