# Databricks notebook source
# MAGIC %run ./2.Test_003

# COMMAND ----------

def get_last_load_time():
    """Read last load time from checkpoint."""
    try:
        df = spark.read.parquet(checkpoint_path)
        return df.agg({"last_load_time": "max"}).collect()[0][0]
    except:
        return None

# COMMAND ----------

def update_checkpoint(ts):
    """Save current load time."""
    spark.createDataFrame([(ts,)], ["last_load_time"]).write.mode("overwrite").parquet(checkpoint_path)

# COMMAND ----------

def log_error(message, stage, details=""):
    ts = datetime.datetime.now()
    error_df = spark.createDataFrame(
        [(ts, stage, message, details)],
        ["timestamp", "stage", "error_message", "details"]
    )
    error_df.write.mode("append").parquet(log_error)

# COMMAND ----------

def load_data(dataset_path, bronze_path, full_load=False):
    try:
        root_path = 'dbfs:/databricks-datasets/retail-org/'
        table_path = 'products/'
        dataset_path = f"{root_path.rstrip('/')}/{table_path.strip('/')}"

        file_format = detect_file_format(dataset_path)
        print(f"📂 Detected format: {file_format.upper()} for {dataset_path}")

        last_ts = get_last_load_time()
        print(f"🕒 Last load time: {last_ts}")
        
        df = (spark.read
                .format(file_format)
                .option("header", "true")
                .load(dataset_path)
                #.withColumn("source_file", F.input_file_name())
                .withColumn("load_timestamp", F.current_timestamp()))

        # If incremental mode and we have a checkpoint
        if not full_load and last_ts:
            df = df.filter(F.col("load_timestamp") > F.lit(last_ts))
            mode = "append"
            print("⚡ Incremental Batch Load")
        else:
            mode = "overwrite"
            print("📦 Full Batch Load")

        df.write.format("parquet").mode(mode).save(bronze_path)

        # Update checkpoint
        update_checkpoint(datetime.datetime.now())
        print(f"✅ {mode.upper()} load completed → {bronze_path}")

    except Exception as e:
        log_error(str(e), f"LOAD_{table_path.strip('/')}", traceback.format_exc())
        print(f"❌ Failed for {table_path}: {e}")



# COMMAND ----------

root_path = 'dbfs:/databricks-datasets/retail-org/'
table_path = 'products/'
dataset_path = f"{root_path.rstrip('/')}/{table_path.strip('/')}"
load_data(dataset_path, bronze_path, full_load=True)   # Run first as full
load_data(dataset_path, bronze_path, full_load=False)  # Next runs as incremental

# COMMAND ----------


