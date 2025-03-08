# Databricks notebook source
spark

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header", "false")\
        .option("inferschema","false")\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/2010_summary.csv")



flight_df.show(5)

# COMMAND ----------

flight_df_header=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","false")\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/2010_summary.csv")



flight_df_header.show(5)

# COMMAND ----------

flight_df_header.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC system ne bataya ki count coloumn ek string hai kyuke hamne inferschema ko "false" kiya hua hai.

# COMMAND ----------

flight_df_header_schema=spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema","true")\
            .option("mode", "FAILFAST")\
                .load("/FileStore/tables/2010_summary.csv")



flight_df_header_schema.show(5)

# COMMAND ----------

flight_df_header_schema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC yaha hamne kyuke inferschema "true" krdiya es lye hamare yaa pe count coloumn integer aagaya.

# COMMAND ----------

spark
