# Databricks notebook source
spark.read.format("json")\
    .option("inferschema","true")\
        .option("mode","PERMEISSIVE")\
            .load("/FileStore/tables/json_file.txt").show()

# COMMAND ----------

# MAGIC %md
# MAGIC TO KNOW ALL THE FILE THAT ARE AVALAIBLE.

# COMMAND ----------

# MAGIC  %fs
# MAGIC  ls /FileStore/tables/

# COMMAND ----------

spark.read.format("json")\
    .option("inferschema","true")\
        .option("mode","PERMEISSIVE")\
            .load("/FileStore/tables/single_file_json.txt").show()

# COMMAND ----------

# MAGIC %md
# MAGIC AGAR HAM LOGO NE EK EXTRA VALUE APNE MULTI-LEVEL JSON FILE ME DEDEYA TB VO EK NEW COLOUMN BANA KE USME STORE HOGA.

# COMMAND ----------

# MAGIC %md
# MAGIC IF YOU WANTED TO WORK WITH MULTI-LINE DATA THEN YOU HAVE TO ADD 

# COMMAND ----------

spark.read.format("json")\
    .option("inferschema","true")\
        .option("multiline","true")\
        .option("mode","PERMEISSIVE")\
            .load("/FileStore/tables/Multi_line_incorrect.txt").show()

# COMMAND ----------

# MAGIC %md
# MAGIC YAAD RAKHO KI JABBHI HAM JSON FILE PASS KRKRHE HAI HAME USKO AS AN LIST PASS KRNA HOGA.

# COMMAND ----------

spark.read.format("json")\
    .option("inferschema","true")\
        .option("multiline","true")\
        .option("mode","PERMEISSIVE")\
            .load("/FileStore/tables/corrupted_json.txt").show()

# COMMAND ----------


