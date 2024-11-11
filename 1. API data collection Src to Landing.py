# Databricks notebook source
# MAGIC %md 
# MAGIC ### This databricks notebook will extract data from Open Charge Map using OCM dev API and store it into a Json file within the landing layer (landing directory in ADLS) 

# COMMAND ----------

# Configuration for Azure Key Vault
kv_scope = "SparshSecretScope"
key_vault_name = "de106kv50215"
key_vault_secret_name = "sparsh-storage-access"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net/secrets/{key_vault_secret_name}"

# COMMAND ----------

# Fetch the secret from Azure Key Vault
storage_account_key = dbutils.secrets.get(scope=kv_scope, key=key_vault_secret_name)

# COMMAND ----------

# Configuration for Landing in ADLS Gen2
storage_account = "de10692367dl"
container_name = "sparsh"
landing_data_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/openchargemap_case_study/Landing/" # Imp Landing layer container name is "Landing"
print(landing_data_path)

# COMMAND ----------

# Set the Spark configuration to use the storage account key
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# listing all the secret scopes
dbutils.secrets.listScopes()

# COMMAND ----------

# command to list all the keys present with in the SparshSecretScope scope
dbutils.secrets.list("SparshSecretScope")

# COMMAND ----------

OCM_API = dbutils.secrets.get('SparshSecretScope', 'Sparsh-Sundriyal-OpenChargeMap-API')
print(OCM_API)

# COMMAND ----------

# MAGIC %md 
# MAGIC The following cells will extract the data from OCM and store it into ADLS as a json file

# COMMAND ----------

# importing all the libraries required for extracting entries from OCM using API
import requests
import json
from datetime import datetime

# COMMAND ----------

# The name of the file is now a string, this resolves the issue of the schema not being inferred by pyspark.

url = "https://api.openchargemap.io/v3/poi/"
headers = {
    "X-API-Key": OCM_API
}
params = {
    "output": "json",
    "countrycode": "US",
    "maxresults": 1000000
}

# Make the GET request
response = requests.get(url, headers=headers, params=params, verify=False)
def time():
    time_stamp=str(datetime.now().strftime("%Y-%m-%dT%H_%M_%SZ"))
    # currentTime = str(datetime.now())
    # new_currentTime = currentTime.replace(" ","-")
    # return new_currentTime
    return time_stamp

if response.status_code == 200:
    data = response.json()
        
    # Convert the JSON data to a string
    json_data = json.dumps(data, indent=4)
        
    # Define the ADLS path
    file_name = f"{landing_data_path}{time()}.json"
        
    # Save the JSON data to ADLS
    dbutils.fs.put(file_name, json_data, overwrite=True)
    
    print(f"Response saved to ADLS at {file_name}")
else:
    print(f"Failed to retrieve data: {response.status_code}")


# COMMAND ----------

dbutils.fs.ls(landing_data_path)

# COMMAND ----------


