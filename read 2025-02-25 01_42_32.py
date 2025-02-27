# Databricks notebook source
spark

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header", "false")\
        .option("inferschema","false")\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/2010_summary-3.csv")
flight_df.show(5)                

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","false")\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/2010_summary-3.csv")
flight_df.show(5) 

# COMMAND ----------

flight_df.printSchema()


# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","true")\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/2010_summary-3.csv")
flight_df.show(5) 

# COMMAND ----------

flight_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header", "falsew")\
        .option("inferschema","true")\
          .schema(my_schema)\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/2010_summary-3.csv")
flight_df.show(5) 

# COMMAND ----------

# MAGIC %md
# MAGIC error would be shown that (my schema) is not defined krke kyuke phele usko define krna pdta h

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType

# COMMAND ----------

my_schema= "id string , name string , age integer"

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header", "true")\
        .schema(my_schema)\
        .option("inferschema","true")\
            .option("mode", "PERMISSIVE")\
                .load("/FileStore/tables/2010_summary-3.csv")
flight_df.show(5) 

# COMMAND ----------

# MAGIC %md
# MAGIC .option("skipRows",2) ----> ye use krke ham rows ko delete krskte h. 
# MAGIC usss 2 ki jgh jitne bhi number type hoga utna rows upar se gayab hoajega.

# COMMAND ----------


