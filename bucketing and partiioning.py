# Databricks notebook source
df=spark.read.format("csv")\
    .option("header","True")\
        .option("mode","FAIFAST")\
        .load("dbfs:/FileStore/tables/bucketing.txt")
df.show()

# COMMAND ----------

df.write.format("csv")\
    .option("header","True")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/partition_bucketing/")\
                .partitionBy("address")\
                    .save()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/partition_bucketing/")

# COMMAND ----------

# MAGIC %md
# MAGIC YAHA PE HAMKO DEKHO KI HAMARE JITNE BHI INDIA , JAPAN, RUSSIA, USA WLE JITNE BHI RECORD HAI VO SAB EK SATH HOGYE H (4RO KA EXEUTION HOCHUKA HAI)
# MAGIC ALAG ALAG FOLDER CREATE HOGYE HAI

# COMMAND ----------

# MAGIC %md
# MAGIC AB HAM DEKHEGE KI YE PARTITION FAIL KAHA HOTA H 

# COMMAND ----------

df.write.format("csv")\
    .option("header","True")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/partition_bucketing_id/")\
                .partitionBy("id")\
                    .save()

# COMMAND ----------

 dbutils.fs.ls("/FileStore/tables/partition_bucketing_id/")

# COMMAND ----------

# MAGIC %md 
# MAGIC YAHA DEKHO KI HAMARE KITNE SAARE CHUNK BAN GYE AISE HI BHUT SAARE CHUNK BAN GYE JISSE HAME EASE NHI HUA (CARDINALITY) MAINTAIN NHI HUA

# COMMAND ----------

# MAGIC %md
# MAGIC HAM PARTITION KREGE ON BAISIS OF AFFRESSS AND USKE ANDAR ON BSIS OF GENDER

# COMMAND ----------

df.write.format("csv")\
    .option("header","True")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/partition_bucketing_id_gender_partition/")\
                .partitionBy("address","gender")\
                    .save()

# COMMAND ----------

 dbutils.fs.ls("/FileStore/tables/partition_bucketing_id_gender_partition/")

# COMMAND ----------

# MAGIC %md
# MAGIC ab eske andar bhi h difference in folder of india,russia usa etcccc

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/partition_bucketing_id_gender_partition/address=INDIA/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

# MAGIC %md
# MAGIC NOW BUCKETING

# COMMAND ----------

df.write.format("csv")\
    .option("header","True")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/bucket_by_id/")\
                .bucketBy(3,"id")\
                    .saveAsTable("bucket_by_id_table")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/bucket_by_id/")

# COMMAND ----------

