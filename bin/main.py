# Databricks notebook source
# MAGIC %run ../config/auth

# COMMAND ----------
# MAGIC %run ../config/processing

# COMMAND ----------

job_params = ["key"]
job_conf = {key: dbutils.widgets.get(key) for key in job_params}
print(f"Var job_conf now contains job config for following keys: {job_params}.")

# COMMAND ----------

### execute pipeline here

# COMMAND ----------
