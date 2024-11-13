# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## This DataBricks notebook will take files from Landing to Raw-Emobility (Bronze layer)
# MAGIC

# COMMAND ----------

# Kv_scope is the secret scope that we create within the databricks workspace
kv_scope = "SparshSecretScope"
key_vault_name = "de106kv50215" # name of the key vault in the resource group
key_vault_secret_name = "sparsh-storage-access" # name of the key present within the key Vault 
key_vault_uri = f"https://{key_vault_name}.vault.azure.net/secrets/{key_vault_secret_name}"

# COMMAND ----------

# Fetch the secret from Azure Key Vault
storage_account_key = dbutils.secrets.get(scope=kv_scope, key=key_vault_secret_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC source_data_path = landing
# MAGIC
# MAGIC sink_data_path = raw-emobility 

# COMMAND ----------

# DBTITLE 1,Configuring ADLS gen 2 storage

storage_account = "de10692367dl" # name of the ADLS gen 2 storage account
container_name = "sparsh" # name of the container within ADLS gen 2 storage account

# path of landing container present in ADLS
# source_data_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/openchargemap_case_study/Landing/"
source_data_path = "/mnt/sparsh_2/openchargemap_case_study/Landing/"
# path of raw emobility container present in ADLS 
# sink_data_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/openchargemap_case_study/raw-emobility/"

# path of the bronze table in databricks 
sink_data_path = "dbfs:/user/hive/warehouse/sparsh_ocm_casestudy/raw-emobility"

checkpoint_path = f"{sink_data_path}/_checkpoints"
print(source_data_path)
print("  \n ")
print(sink_data_path)
print("\n")
print(checkpoint_path)

# COMMAND ----------

# Set the Spark configuration to use the storage account key
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# checking the connect by listing all the files in the sink container (raw emobility)
# this will return empty when run for the first time bcz the layer is empty and we will transport data into the container
dbutils.fs.ls(source_data_path)

# COMMAND ----------

print(source_data_path)

# COMMAND ----------

# landing_file_path is the path of all the json files present in landing container
import re 

files = dbutils.fs.ls(source_data_path)
json_files = []
pattern = re.compile(r".*\.json$")

for file in files:
    if re.match(pattern, file.path):
        json_files.append([file.path, file.modificationTime])

print(f"all the json fiels are \n {json_files}")

# Ensure there are JSON files before attempting to find the latest one
if json_files:
    latest_file = max(json_files, key=lambda x: x[1])
    landing_file_path = latest_file[0]
    print(f"the final path of the file {landing_file_path}")
else:
    print("No JSON files found in the specified path.")


# COMMAND ----------

df = spark.read.json(path=landing_file_path, multiLine=True)
df.display()
df.count()

# COMMAND ----------

df.select("ID").count()

# COMMAND ----------

from pyspark.sql.functions import input_file_name, to_timestamp, lit, current_timestamp, date_format
from datetime import datetime

df = df.withColumn("storage_path",input_file_name())\
    .withColumn("DateCreated", to_timestamp(df["DateCreated"], "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
    .withColumn("source_name",lit("OCM_API"))\
    .withColumnRenamed("DateCreated","created_timestamp")\
    .withColumn("ingest_timestamp",to_timestamp(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'")))

# COMMAND ----------

# this command saves the dataframe as delta table named raw_emobility_sparsh
df.write.format("delta").mode("overwrite").saveAsTable("`raw_emobility_bronze_sparsh`")

# COMMAND ----------


