# Databricks notebook source
# MAGIC %md
# MAGIC # Silver cleansing
# MAGIC
# MAGIC Reads a bronze table, casts each column to the type declared in
# MAGIC configs/silver/tables.yml, drops rows with a null primary key, and
# MAGIC writes the result to the corresponding silver Delta table.

# COMMAND ----------

import sys
from pathlib import Path

NOTEBOOK_DIR = Path.cwd()
SRC_DIR = NOTEBOOK_DIR.parent.parent

sys.path.append(str(SRC_DIR))

from silver.clean import clean_data

# COMMAND ----------

dbutils.widgets.text("table_name", "")

table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

clean_data(spark, table_name)
