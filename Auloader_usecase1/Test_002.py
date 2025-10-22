# Databricks notebook source
# MAGIC %run ./Test_001

# COMMAND ----------

root_path = 'dbfs:/databricks-datasets/retail-org/' 
table_path = 'products/'

# Build full dataset path
full_table_path = f"{root_path.rstrip('/')}/{table_path.strip('/')}/" #here if don't use / at end as well it works as dbutils treats it as a folder


# List all files inside that table folder
files_info = dbutils.fs.ls(full_table_path)  #here we use full path so dir as False, if we give root path then True for is dir

# Convert to list of dicts for DataFrame creation
file_details = [{
    "root_path": root_path,
    "table_path": full_table_path,
    "file_name": f.name,
    "file_path": f.path,
    "size_bytes": f.size,
    "isDir": f.isDir()
} for f in files_info]

# Convert to Spark DataFrame
df_files = spark.createDataFrame(file_details)

# Convert Spark DF to Pandas
pdf = df_files.toPandas()  # To get into table format and text as well if we use display truncate is not working,show is not getting in table format

# Display as a rich HTML table (no truncation)
display(pdf)



# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/retail-org/products/'))

# COMMAND ----------


