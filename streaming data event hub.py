# Databricks notebook source
dbutils.fs.mount(source = "wasbs://new@zee.blob.core.windows.net",mount_point = "/mnt/zeeshan1", extra_configs = {"fs.azure.account.key.zee.blob.core.windows.net":"YlBjJI7gIJpTUQ5ELXbqiqI/T/WhLRDi03zTBEiVU30O0O9zXwkWqKULPst5k+5cors7NYTbyw/v+AStJ3bIBw=="})

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## straming data from event hub 

# COMMAND ----------

from pyspark.sql.types import *

import pyspark.sql.functions as F




conection_string = "Endpoint=sb://hubeventzee.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=fQjeT/TVaW1diuHEqljws0OZhF9CVcQBf+AEhIscSs4=;EntityPath=event" # dbutils.secrets.get('iot','iothub-cs') # IoT Hub connection string (Event Hub Compatible)
ehConf = {}

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conection_string)
ehConf['ehName'] = '$Default'




json_schema = StructType([StructField('idDrink', StringType(), True),StructField('strDrink',StringType(), True), StructField('dateModified', StringType(), True)])


df = spark.readStream.format('eventhubs').options(**ehConf).load()

# body_df = df.withColumn('body',df.body.cast('string')).select('body')

# display(body_df)

json_df = df.withColumn('body',F.from_json(df.body.cast('string'), json_schema))

# display(json_df)

df3 = json_df.select(F.col("body.idDrink"), F.col("body.strDrink"), F.col("body.dateModified"))


display(df3)


# df3.writeStream.format(csv).

# COMMAND ----------



# COMMAND ----------

df9 = spark.read.format('avro').load("/mnt/zeeshan1/hubeventzee/event/0/2023/**/**/**/**/*.avro")

# COMMAND ----------

display(df9)
