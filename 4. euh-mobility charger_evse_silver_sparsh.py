# Databricks notebook source
# MAGIC %md 
# MAGIC # This notebook will create the charger_evse_silver_sparsh table 

# COMMAND ----------

raw_emobility_bronze_sparsh = "dbfs:/user/hive/warehouse/raw_emobility_bronze_sparsh"
master_df = spark.read.format("delta").load(raw_emobility_bronze_sparsh)
master_df.display()
master_df.count()

# COMMAND ----------

charger_location_silver_sparsh = "dbfs:/user/hive/warehouse/charger_location_silver_sparsh"
charger_location_silver_sparsh_df = spark.read.format("delta").load(charger_location_silver_sparsh)
charger_location_silver_sparsh_df.display()
charger_location_silver_sparsh_df.count()

# COMMAND ----------

from pyspark.sql.functions import explode, col, lit
from pyspark.sql.types import IntegerType, StringType

# Explode the Connections array into separate rows
df_exploded = master_df.withColumn("Connections", explode(col("Connections")))

# Select the required columns and rename them
df_result = df_exploded.select(
    col("Connections.ID").cast(IntegerType()).alias("evse_id"),
    col("ID").cast(IntegerType()).alias("location_id_charger_evse"),
    col("Connections.ID").cast(StringType()).alias("source_evse_id"),
    col("UUID").cast(StringType()).alias("chargepoint_id")
)\
    .withColumn("operator_evse_id",lit(None).cast(StringType()))\
    .withColumn("ocpi_evse_id",lit(None).cast(StringType()))\
    .withColumn("manufacturer",lit(None).cast(StringType()))\
    .withColumn("model",lit(None).cast(StringType()))

df_result.display()
df_result.count()


# COMMAND ----------

from pyspark.sql.functions import col

charger_evse_silver_sparsh_df = df_result.join(charger_location_silver_sparsh_df,df_result["location_id_charger_evse"] == charger_location_silver_sparsh_df["location_id"],"left")\
  .select(col("evse_id"),
          col("location_id"),
          col("source_evse_id"),
          col("chargepoint_id"),
          col("source"),
          col("operator_evse_id"),
          col("ocpi_evse_id"),
          col("manufacturer"),
          col("model"),
          col("latitude"),
          col("longitude"),
          col("created"),
          col("modified"),
          col("processed"),
          col("commissioned"),
          col("decommissioned")
          )

charger_evse_silver_sparsh_df.display()
charger_evse_silver_sparsh_df.count()


# COMMAND ----------

charger_evse_silver_sparsh_df = charger_evse_silver_sparsh_df\
  .select("evse_id","location_id","source","source_evse_id","operator_evse_id","ocpi_evse_id","chargepoint_id","manufacturer","model","latitude","longitude","created","modified","processed","commissioned","decommissioned")

# COMMAND ----------

# saving the table under the name charger_evse_silver_sparsh
charger_evse_silver_sparsh_df.write.format("delta").mode("overwrite").saveAsTable("`charger_evse_silver_sparsh`")
