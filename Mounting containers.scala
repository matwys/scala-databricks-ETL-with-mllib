// Databricks notebook source
// MAGIC %md
// MAGIC ## This is notebook for mounting container
// MAGIC Go to https://\<databricks-instance\>#secrets/createScope. This URL is case sensitive; scope in createScope must be uppercase.

// COMMAND ----------

val applicationId = dbutils.secrets.get(scope="databricks-secrets777",key="applicationid")
val tenantId = dbutils.secrets.get(scope="databricks-secrets777",key="tenantid")
val secret = dbutils.secrets.get(scope="databricks-secrets777",key="secret")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 01 Linear Regression Project (cruise-ship)

// COMMAND ----------

val containerName = "cruise-ship"
val accountName = "datalake777777"
val mountPoint = "/mnt/cruise-ship"


// COMMAND ----------

val configs = Map("fs.azure.account.auth.type" ->  "OAuth",
          "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id" -> applicationId,
          "fs.azure.account.oauth2.client.secret" -> secret,
          "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/${tenantId}/oauth2/token")


// COMMAND ----------

dbutils.fs.mount(
  source = "abfss://${containerName}@${accountName}.dfs.core.windows.net/",
  mountPoint = mountPoint,
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %python
// MAGIC application_id = dbutils.secrets.get(scope="databricks-secrets777",key="applicationid")
// MAGIC tenant_id = dbutils.secrets.get(scope="databricks-secrets777",key="tenantid")
// MAGIC secret = dbutils.secrets.get(scope="databricks-secrets777",key="secret")
// MAGIC
// MAGIC container_name = "cruise-ship"
// MAGIC account_name = "datalake777777"
// MAGIC mount_point = "/mnt/cruise-ship"
// MAGIC
// MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
// MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC           "fs.azure.account.oauth2.client.id": application_id,
// MAGIC           "fs.azure.account.oauth2.client.secret": secret,
// MAGIC           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
// MAGIC  
// MAGIC dbutils.fs.mount(
// MAGIC   source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
// MAGIC   mount_point = mount_point,
// MAGIC   extra_configs = configs)
