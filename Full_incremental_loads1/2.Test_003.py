# Databricks notebook source
# MAGIC %run ./2.Test_002

# COMMAND ----------

def log_error(message, stage, details=""):
    ts = datetime.datetime.now()
    error_df = spark.createDataFrame(
        [(ts, stage, message, details)],
        ["timestamp", "stage", "error_message", "details"]
    )
    error_df.write.mode("append").parquet(log_error)

# COMMAND ----------

def detect_file_format(path):
    """Detect most common file extension and map to Spark format."""
    try:
        files = dbutils.fs.ls(path)
        exts = [os.path.splitext(f.name.lower())[1].lstrip(".") for f in files if not f.isDir()]
        format_map = {
            "csv": "csv",
            "json": "json",
            "parquet": "parquet",
            "txt": "text",
            "log": "text",
            "gz": "text"
        }
        if exts:
            most_common = Counter(exts).most_common(1)[0][0]
            return format_map.get(most_common, "csv")   # ✅ Default to CSV
        else:
            return "csv"
    except Exception as e:
        log_error(str(e), f"FORMAT_DETECTION/{table_path.strip('/')}", traceback.format_exc())
        return "csv"

# COMMAND ----------


