# Databricks notebook source
# MAGIC %md 
# MAGIC # This notebook will create the charger_connector_silver_sparsh table 

# COMMAND ----------

raw_emobility_bronze_sparsh = "dbfs:/user/hive/warehouse/raw_emobility_bronze_sparsh"
master_df = spark.read.format("delta").load(raw_emobility_bronze_sparsh)
master_df.display()
master_df.count()

# COMMAND ----------

charger_evse_silver_sparsh = "dbfs:/user/hive/warehouse/charger_evse_silver_sparsh"
charger_evse_silver_sparsh_df = spark.read.format("delta").load(charger_evse_silver_sparsh)
charger_evse_silver_sparsh_df.display()
charger_evse_silver_sparsh_df.count()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, array, struct, udf, explode, size, col
from pyspark.sql.types import IntegerType, StringType, ArrayType, StructType, StructField, DecimalType


# Define a UDF to generate evse_ids and source_evse_ids based on connection ID and quantity
def generate_evse_ids_and_source(connections):
    evse_ids = []
    source_evse_ids = []
    ConnectionType_FormalName_list = []
    ConnectionTypeID_list = []
    ConnectionType_Title_list = []
    CurrentType_ID_list = []
    Voltage_list = []
    Amps_list = []
    PowerKW_list = []
    for connection in connections:
        connection_id = connection['ID'] if 'ID' in connection else None

        quantity = connection['Quantity'] if connection['Quantity'] is not None else 1

        ConnectionType_FormalName = connection['ConnectionType']['FormalName'] if 'ConnectionType' in connection and 'FormalName' in connection['ConnectionType'] else None 

        ConnectionTypeID = connection['ConnectionTypeID'] if 'ConnectionTypeID' in connection else None

        ConnectionType_Title = connection['ConnectionType']['Title'] if 'ConnectionType' in connection and 'Title' in connection['ConnectionType'] else None

        CurrentType_ID = connection['CurrentTypeID'] if 'CurrentTypeID' in connection else None

        Voltage = connection['Voltage'] if 'Voltage' in connection else None
        Amps = connection['Amps'] if 'Amps' in connection else None
        PowerKW = connection['PowerKW'] if 'PowerKW' in connection else None
        for i in range(1, quantity + 1):
            evse_ids.append(f"{connection_id}{i}")
            source_evse_ids.append(connection_id)
            ConnectionType_FormalName_list.append(ConnectionType_FormalName)
            ConnectionTypeID_list.append(ConnectionTypeID)
            ConnectionType_Title_list.append(ConnectionType_Title)
            CurrentType_ID_list.append(CurrentType_ID)
            Voltage_list.append(Voltage)
            Amps_list.append(Amps)
            PowerKW_list.append(PowerKW)
    return list(zip(evse_ids, source_evse_ids, ConnectionType_FormalName_list, ConnectionTypeID_list, ConnectionType_Title_list, CurrentType_ID_list, Voltage_list, Amps_list, PowerKW_list))

generate_evse_ids_and_source_udf = udf(generate_evse_ids_and_source, ArrayType(StructType([
    StructField("evse_id", StringType(), True),
    StructField("source_evse_id", IntegerType(), True),
    StructField("ConnectionType_FormalName", StringType(), True),
    StructField("ConnectionTypeID", StringType(), True),
    StructField("ConnectionType_Title", StringType(), True),
    StructField("CurrentType_ID", IntegerType(), True),
    StructField("Voltage", IntegerType(), True),
    StructField("Amps", IntegerType(), True),
    StructField("PowerKW", StringType(), True),
])))


# filtering the data to exclude the rown which have an empty connections array
filtered_master_df = master_df.filter(size(col("Connections")) > 0)

# Create a new DataFrame with the generated connector_ids and evse_ids and other data
evse_df = filtered_master_df.withColumn("evse_data", explode(generate_evse_ids_and_source_udf(col("Connections"))))\
    .select(
        col("evse_data.evse_id").cast(IntegerType()).alias("connector_id"),# make this connector id 
        col("evse_data.source_evse_id").alias("evse_id"),# make this evse id
        col("evse_data.ConnectionType_FormalName").alias("source_connector_id"),
        col("evse_data.ConnectionTypeID").alias("ocpi_connector_id"),
        col("evse_data.ConnectionType_Title").alias("connector_type"),
        col("evse_data.CurrentType_ID").alias("phase"),
        col("evse_data.Voltage").alias("voltage"),
        col("evse_data.Amps").alias("amperage"),
        col("evse_data.PowerKW").cast(DecimalType(7,2)).alias("power_kw")
    )\
      .withColumn("operator_connector_id",lit(None).cast(StringType()))

# Show the result
evse_df.display(truncate=False)
evse_df.count()


# COMMAND ----------

# finding the current type title i.e., power type
from pyspark.sql.functions import size, col, explode
# filtering the data to exclude the rown which have an empty connections array
filtered_master_df = master_df.filter(size(col("Connections")) > 0)

exlpoded_connections_df = filtered_master_df.withColumn("connection",explode("Connections")).select("connection")

# exlpoded_connections_df.display()
type_value_df = exlpoded_connections_df.select("Connection.ID", "Connection.CurrentType.Title")\
  .withColumnRenamed("Title","power_type")
type_value_df.display()

# COMMAND ----------

# joining the powertype dataframe with the charger connector data frame 
evse_df = evse_df.join(type_value_df,evse_df["evse_id"] == type_value_df["ID"],"left").drop("ID")
evse_df.display()


# COMMAND ----------

# taking the required columns form charger_evse table to join with charger_connector table to fill in all the details 
from pyspark.sql.functions import col

selected_charger_evse_df = charger_evse_silver_sparsh_df.select(col("evse_id"),
                                                                col("created"),
                                                                col("modified"),
                                                                col("processed"),
                                                                col("source"))\
                                                                  .withColumnRenamed("evse_id","evse_id_2")

charger_connector_silver_sparsh_df = evse_df.join(selected_charger_evse_df,evse_df["evse_id"] == selected_charger_evse_df["evse_id_2"],"left").drop("evse_id_2")

charger_connector_silver_sparsh_df.display()
charger_connector_silver_sparsh_df.count()


# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# hashmap for mapping the phase id to phase value it 1,2 or 3
phase_mapping = {
    10: 1,
    20: 3,
    30: 1,
    None: 1
}

# The function responsible for transforming the values
def transform_phase_value(phase):
    return phase_mapping.get(phase, 1)  # Default to 1 if the phase is not in the hashmap

# Register the UDF
transform_phase_udf = udf(transform_phase_value, IntegerType())


# COMMAND ----------

# regex for replacing all entires in power_type column to AC or DC
from pyspark.sql.functions import regexp_replace

charger_connector_silver_sparsh_df = charger_connector_silver_sparsh_df\
  .withColumn("power_type", regexp_replace("power_type", "AC \\(.*\\)", "AC"))\
  .withColumn("phase", transform_phase_udf(charger_connector_silver_sparsh_df["phase"]))

# COMMAND ----------

charger_connector_silver_sparsh_df = charger_connector_silver_sparsh_df\
  .select("connector_id","evse_id","source","source_connector_id","operator_connector_id","ocpi_connector_id","connector_type","power_type","phase","voltage","amperage","power_kw","created","modified","processed")
charger_connector_silver_sparsh_df.display()

# COMMAND ----------

# saving the table under the name charger_connector_silver_sparsh
charger_connector_silver_sparsh_df.write.format("delta").mode("overwrite").saveAsTable("`charger_connector_silver_sparsh`")
