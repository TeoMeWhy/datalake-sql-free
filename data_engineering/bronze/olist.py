# Databricks notebook source
def ingestion_table(file_path):

    table_name = (file_path.split("/")[-1]
                               .replace("olist_", "")
                               .replace(".csv", "")
                               .replace("_dataset", ""))
    (spark.read
          .csv(file_path, header=True, multiLine=True)
          .coalesce(1)
          .write
          .mode("overwrite")
          .format("delta")
          .option("overwriteSchema", "true")
          .saveAsTable(f"bronze_olist.{table_name}"))

# COMMAND ----------

file_paths = [ i.path for i in dbutils.fs.ls("/mnt/datalake/raw/olist")]

for f in file_paths:
    print(f)
    ingestion_table(f)
