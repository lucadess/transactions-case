# Databricks notebook source
# MAGIC %md
# MAGIC # Gold generation
# MAGIC
# MAGIC Selects the GoldGenerator registered for the given table, which reads
# MAGIC its silver sources, joins/filters/transforms them, and writes the
# MAGIC result to the corresponding gold Delta table.

# COMMAND ----------

import sys
from pathlib import Path

NOTEBOOK_DIR = Path.cwd()
SRC_DIR = NOTEBOOK_DIR.parent.parent

sys.path.append(str(SRC_DIR))

from gold.factory import get_gold_generator

# COMMAND ----------

dbutils.widgets.text("table_name", "")

table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

get_gold_generator(spark, table_name).generate()
