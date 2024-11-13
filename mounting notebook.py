# Databricks notebook source
# Configuration for Azure Key Vault
kv_scope = "SparshSecretScope"
key_vault_name = "de106kv50215"
key_vault_secret_name = "sparsh-storage-access"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net/secrets/{key_vault_secret_name}"
storageAccountName = "de10692367dl"

# COMMAND ----------

# Fetch the secret from Azure Key Vault
storage_account_key = dbutils.secrets.get(scope=kv_scope, key=key_vault_secret_name)
sasToken = dbutils.secrets.get(scope=kv_scope, key="Sparsh-Sundriyal-Container-Token")

# COMMAND ----------

dbutils.secrets.list("SparshSecretScope")

# COMMAND ----------

# mounting the adls to this workspace
dbutils.fs.mount(
    source = "wasbs://sparsh@de10692367dl.blob.core.windows.net/",
    mount_point = "/mnt/sparsh_2/",
    extra_configs={"fs.azure.account.key.de10692367dl.blob.core.windows.net":storage_account_key}
)

# COMMAND ----------

blobContainerName = "sparsh"
mountPoint = "/mnt/sparsh_2/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

dbutils.fs.ls("/mnt/sparsh_2/")

# COMMAND ----------


