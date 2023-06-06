# Databricks notebook source
# Use dbutils secrets to get Snowflake credentials.
# user = dbutils.secrets.get("data-warehouse", "<snowflake-user>")
# password = dbutils.secrets.get("data-warehouse", "<snowflake-password>")

options = {
  "sfUrl": "ef16868.central-india.azure.snowflakecomputing.com",
  "sfUser": 'mohammed',
  "sfPassword": 'P@ssw0rd',
  "sfDatabase": "ZEE_DB",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

# Generate a simple dataset containing five values and write the dataset to Snowflake.
df1.write.format("snowflake").options(**options).option("dbtable", "umaira").save()

# COMMAND ----------

# Read the data written by the previous cell back.
df = spark.read.format("snowflake").options(**options).option("dbtable", "OBJECT_PRIVILEGES").load()

display(df)

# COMMAND ----------

# Write the data to a Delta table

df.write.format("delta").saveAsTable("sf_ingest_table")

# COMMAND ----------

  spark.read.format()

# COMMAND ----------

df = spark.read.parquet('dbfs:/dbfs/user/hive/warehouse/employee_demo_df/*.parquet')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employee_demo_df

# COMMAND ----------

df1 = spark.sql("SELECT * FROM default.employee_demo_df")

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.df1.write.format("snowflake").options(**options).option("dbtable", "umaira").save()

# COMMAND ----------

df2= spark.read.csv('dbfs:/FileStore/IPL_Matches_2008_2020.csv', header=True, inferSchema=True)

# COMMAND ----------

df2.write.format("snowflake").options(**options).option("dbtable", "IPL").save()
