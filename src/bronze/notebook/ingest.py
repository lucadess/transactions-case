# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC Reads the raw staged JSON pages for a table from the staging UC volume
# MAGIC and writes them into the corresponding bronze Delta table.

# COMMAND ----------

import sys
from pathlib import Path

# This notebook lives at src/bronze/notebook/, two levels below src/.
# Databricks Repos sets a notebook's cwd to its own directory, so we derive
# src/ from there to make the bronze/utils imports work regardless of where
# the repo is checked out.
NOTEBOOK_DIR = Path.cwd()
SRC_DIR = NOTEBOOK_DIR.parent.parent

sys.path.append(str(SRC_DIR))

from bronze.ingest import ingest_data

# COMMAND ----------

dbutils.widgets.text("table_name", "")

table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

ingest_data(spark, table_name)
