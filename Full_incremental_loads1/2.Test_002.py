# Databricks notebook source
# MAGIC %run ./2.Test_001

# COMMAND ----------

root_path = 'dbfs:/databricks-datasets/retail-org/'
table_path = 'products/'
dataset_path = f"{root_path.rstrip('/')}/{table_path.strip('/')}"

# COMMAND ----------

bronze_path=f"/Volumes/checkpointpath_inc_full/chpointpath_inc_full/checkpointdata/{root_path.split('/')[-2]}/{table_path.strip('/')}"
checkpoint_path =f"/Volumes/checkpointpath_inc_full/chpointpath_inc_full/checkpointdata/{root_path.split('/')[-2]}/{table_path.strip('/')}"
log_error=f"/Volumes/log_error_full_incrload/log_error_full_incrload/log_error_data/{root_path.split('/')[-2]}/{table_path.strip('/')}"


# COMMAND ----------

print(type(bronze_path))

# COMMAND ----------


