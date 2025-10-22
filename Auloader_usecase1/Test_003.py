# Databricks notebook source
# MAGIC %run ./Test_002

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp,os
from collections import Counter
import datetime, traceback

# COMMAND ----------

# Base locations for checkpoints and error logs
checkpoint_base_path = "/Volumes/autoloader_location/autoloaderloc/autoloader_data"
error_log_path = "/Volumes/error_log/error_log/error_log_data"
bronze_write_location=f"/Volumes/bronze_write/bronze_schema_write/bronze_data/{root_path.split('/')[-2]}/{table_path.strip('/')}"
schema_path=f"/Volumes/schema/schema/schema_data/{root_path.split('/')[-2]}/{table_path.strip('/')}"
#root_path = 'dbfs:/databricks-datasets/retail-org/' --> ['dbfs:', 'databricks-datasets', 'retail-org', '']

# COMMAND ----------

def detect_file_format(path):
    """Detect file format from files in a given path"""
    files = dbutils.fs.ls(path)
    exts = [os.path.splitext(f.name.lower())[1].lstrip(".") for f in files if not f.isDir()]
    
    # Common extensions → Spark format mapping
    format_map = {
        "csv": "csv",
        "json": "json",
        "parquet": "parquet",
        "txt": "text",
        "log": "text",
        "gz": "text"
    }
    if exts:
        most_common = Counter(exts).most_common(1)[0][0]  #counter gives all the counts of the files like csv count and all, from that it will take the first one as csv or parquet..
        return format_map.get(most_common, "text")  # default to 'text'
    else:
        return "text"  # fallback

# COMMAND ----------

def log_error(error_message, stage, details=""):
    from datetime import datetime
    log_df = spark.createDataFrame(
        [(datetime.now(), stage, error_message, details)],
        ["timestamp", "stage", "error_message", "details"]
    )
    log_df.write.mode("append").parquet(error_log_path)

# COMMAND ----------

try:
    dataset_path = full_table_path
    file_format = detect_file_format(dataset_path)

    print(f"📂 Dataset: {dataset_path}")
    print(f"🔍 Detected Format: {file_format}")

    df_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_path)
        .option("recursiveFileLookup", "true")
        .load(dataset_path)
        #.withColumn("source_file", input_file_name())
        #.withColumn("load_timestamp", current_timestamp())
    )

    (df_stream.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint_base_path)
        .option("path", bronze_write_location)
        .outputMode("append")
        .trigger(once=True)
        .start()
    )

    print("✅ Streaming started successfully!")

except Exception as e:
    log_error(str(e), f"AUTOLOADER_{table_path.strip('/')}", traceback.format_exc())
    print(f"🚫 Failed for dataset: {table_path.strip('/')}")

# COMMAND ----------

df=spark.read.parquet(error_log_path)

# COMMAND ----------

latest_error = (
    df
    .orderBy(df.timestamp.desc())
    .limit(1)
)
display(latest_error)

# COMMAND ----------

df=spark.read.parquet(bronze_write_location)

# COMMAND ----------

display(df)
