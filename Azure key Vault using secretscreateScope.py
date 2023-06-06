# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## list down the secrete scope

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

dbutils.fs.mount(source = "wasbs://csv@zee.blob.core.windows.net",mount_point = "/mnt/checknew",extra_configs = {"fs.azure.account.key.zee.blob.core.windows.net":dbutils.secrets.get('AKV_serete_check', 'adlsaccesskey')})

# COMMAND ----------

dbutils.secrets.get('AKV_serete_check', 'adlsaccesskey')

# COMMAND ----------

dbutils.fs.ls('/mnt/checknew')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # databricked cli to secrete scope create and store
# MAGIC

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://row@zee.blob.core.windows.net",
    mount_point = "/mnt/zeesasadls",
    extra_configs = {"fs.azure.sas.row.zee.blob.core.windows.net":dbutils.secrets.get("dbscopedemo", "dbkeysasadls")})

# COMMAND ----------

dbutils.secrets.get("dbscopedemo", "dbkeysasadls")

# COMMAND ----------



# COMMAND ----------


