# Databricks notebook source
# MAGIC %fs
# MAGIC ls FileStore/tables/

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

light_df=spark.read.format("csv")\
    .option("header", "false")\
        .option("inferschema","true")\
            .option("mode", "PERMESSIVE")\
                .load("dbfs:/FileStore/tables/real.txt")
light_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ALIASING

# COMMAND ----------

light_df.select(col("_c0").alias("employee"),"_c1","_c2").show()

# COMMAND ----------

light_df.select(col("_c0").alias("employee"),col("_c1").alias("age"),"_c2").show()

# COMMAND ----------

light_df.select(col("_c0").alias("employee"),col("_c1").alias("age"),col("_c2").alias("salary")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC FILTER

# COMMAND ----------

light_df.where(col("_c1")>15).show()

# COMMAND ----------

light_df.filter(col("_c1")>15).show()

# COMMAND ----------

# MAGIC %md
# MAGIC bracket k dhyan rakhna pdega

# COMMAND ----------

light_df.where((col("_c2")>15000) & (col("_c1")<10)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC LITERAL- ek coulumn ham add keskte hai jisme by default ek value set krskte hai.

# COMMAND ----------

light_df.select("*", lit("sharma")).show()

# COMMAND ----------

light_df.select("*", lit("sharma").alias("Surname")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ADDING COLUMN
# MAGIC

# COMMAND ----------

light_df.withColumn("Sur_Name",lit("vishwakarma")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC RENAME COLUMN

# COMMAND ----------

light_df.withColumnRenamed("_c1","employee").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ab agar mujhe ye data save krna h jisme maine name ko chage kiya hai tab

# COMMAND ----------

new_light_df = light_df.withColumnRenamed("_c1","employee").show()

# COMMAND ----------

# MAGIC %md
# MAGIC aise agar naya column ko save kiye tab usko new words deke save kro jaise maine 'new_light_df' diya hai ab vo renamed table mere es nye word me save hojae ga

# COMMAND ----------

# MAGIC %md
# MAGIC CASTING DATA TYPES - ham esme ab datatypes ko change krenge jaise string ko integer bana denge.......

# COMMAND ----------

light_df.printSchema()

# COMMAND ----------

light_df.withColumn('_c1',col("_c1").cast('integer')).printSchema()

# COMMAND ----------

light_df.withColumn('_c1',col("_c1").cast('integer'))\
        .withColumn('_c0',col('_c0').cast('long'))\
    .printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC REMOVE COLUMN

# COMMAND ----------

light_df.drop("_c3","_c1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC SPARK SQL

# COMMAND ----------

spark.sql("""
          select * from light_df where salary>15000 and age<18
""").show()

# COMMAND ----------

spark.sql("""
          select *, "kumar" as last_name from light_df where salary>15000 and age<18
""").show()

# COMMAND ----------

spark.sql("""
          select *, "kumar" as last_name, concat(name,last_name) as full_name from light_df where salary>15000 and age<18
""").show()

# COMMAND ----------

