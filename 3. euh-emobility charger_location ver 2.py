# Databricks notebook source
# MAGIC %md 
# MAGIC # This notebook will take information from Raw-Emobility table and then will form the silver layer table called Charger_Location_sparsh
# MAGIC

# COMMAND ----------

raw_emobility_bronze_sparsh = "dbfs:/user/hive/warehouse/raw_emobility_bronze_sparsh"
master_df = spark.read.format("delta").load(raw_emobility_bronze_sparsh)
master_df.display()
master_df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_emobility_bronze_sparsh 
# MAGIC order by ID

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.types import StringType, IntegerType

charger_location_df = master_df.select(
  col("ID").cast(IntegerType()).alias("location_id"),
  col("UUID").alias("operator_location_id"),
  col("ID").cast(StringType()).alias("source_location_id"),
  col("AddressInfo.Country.ISOCode").alias("country_code"),
  concat_ws(" ", col("AddressInfo.AddressLine1"), col("AddressInfo.AddressLine2")).alias("address"),
  col("AddressInfo.Town").alias("city"),
  col("AddressInfo.StateOrProvince").alias("county"),
  col("AddressInfo.Postcode").alias("postal_code"),
  col("AddressInfo.Latitude").alias("latitude"),
  col("AddressInfo.Longitude").alias("longitude"),
  col("OperatorInfo.Title").alias("operator"),
  col("OperatorInfo.Title").alias("owning_company"),
  col("created_timestamp").alias("created"),
  col("ingest_timestamp").alias("modified"),
  col("ingest_timestamp").alias("processed"),
  col("DatePlanned").alias("commissioned")
)\
  .withColumn("status",lit("active"))\
  .withColumn("source",lit("raw_emobility_bronze_sparsh"))\
  .withColumn("decommissioned",lit(None).cast("date"))\
  .withColumn("location_type",lit("PUBLIC"))\
  .withColumn("location_sub_type",lit(None).cast("string"))\
  .withColumn("name",lit(None).cast("string"))



# COMMAND ----------

charger_location_df.display()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from tzfpy import get_tz

# Define the UDF to get timezone
def get_timezone(longitude, latitude):
    try:
        return get_tz(longitude, latitude)
    except Exception as e:
        return None

# Register the UDF
get_tz_udf = udf(get_timezone, StringType())

# Apply the UDF to add the timezone column
charger_location_df = charger_location_df.withColumn('timezone', get_tz_udf(charger_location_df['longitude'], charger_location_df['latitude']))

# Show the resulting DataFrame
charger_location_df.display()
charger_location_df.count()


# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_timestamp, to_date


charger_location_df = charger_location_df\
    .withColumn("commissioned", to_date(to_timestamp(charger_location_df["commissioned"], "yyyy-MM-dd'T'HH:mm:ss'Z'")))\
    .withColumn("decommissioned", to_date(to_timestamp(charger_location_df["decommissioned"], "yyyy-MM-dd'T'HH:mm:ss'Z'")))

# COMMAND ----------

# counting the number of columns in the dataframe
num_columns = len(charger_location_df.columns)
print(f"The DataFrame has {num_columns} columns.")

# COMMAND ----------

charger_location_df = charger_location_df.select("location_id","operator_location_id","source_location_id","location_type","location_sub_type","name","country_code","address","city","county","postal_code","latitude","longitude","timezone","status","operator","owning_company","source","created","modified","processed","commissioned","decommissioned")

# COMMAND ----------

charger_location_df.display()
charger_location_df.count()

# COMMAND ----------

# this command saves the dataframe as delta table named charger_location_sparsh
charger_location_df.write.format("delta").mode("overwrite").saveAsTable("`charger_location_silver_sparsh`")
