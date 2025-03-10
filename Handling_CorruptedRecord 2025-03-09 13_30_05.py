# Databricks notebook source
 id_df=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","true")\
            .option("mode", "PERMISSIVE")\
                .load("/FileStore/tables/id.csv")
id_df.show()

# COMMAND ----------

 id_df=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","true")\
            .option("mode", "DROPMALFROMED")\
                .load("/FileStore/tables/id.csv")
id_df.show()

# COMMAND ----------

 id_df=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","true")\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/id.csv")
id_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC TO HANDLE CORRUPTED RECORD WE NEED TO MAKE SCHEMA FIRST

# COMMAND ----------

from pyspark.sql.types import StringType,StructField,IntegerType,StringType

# COMMAND ----------

id_schema =  " id integer,name string, age integer , salary integer, address string , nominee string, corrupt string "

# COMMAND ----------

 id_df=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","true")\
            .schema(id_schema)\
            .option("mode", "PERMISSIVE")\
                .load("/FileStore/tables/id.csv")
id_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC id_df.show(truncate = false)
# MAGIC this statement sab dikha dega jo bhi ... dot dot bana tha vi na ban ke pura dikha dega.

# COMMAND ----------

# MAGIC %md
# MAGIC STORE CORRUPTED RECORD
# MAGIC
# MAGIC  .option("BadRecordPath", "/FileStore/tables/badrecord" )
# MAGIC  
# MAGIC   ye statament ek nye file bana ke usme corrupted record store krke dedega.
# MAGIC   that file will always be in JSON format.

# COMMAND ----------

 id_df=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","true")\
            .schema(id_schema)\
            .option("mode", "PERMISSIVE")\
              .option("BadRecordPath", "/FileStore/tables/badrecord" )\
                .load("/FileStore/tables/id.csv")
id_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC kitni file hai vo check krne k liye

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

badData_show_df= spark.read.format("json").load("path of that file jo ham (%fs ls) upar hai jaise uske through nikale ge aur paste krdenge ")
badData_show_df.show()
