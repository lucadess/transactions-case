# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: API ingestion
# MAGIC
# MAGIC Pulls the `customers`, `transactions` and `operation_status_code` endpoints
# MAGIC page by page and lands the raw JSON responses in the staging UC volume.
# MAGIC Resumable: rerunning skips any page already written to the volume.

# COMMAND ----------

import sys
from pathlib import Path

# This notebook lives at src/staging/notebook/, three levels below the repo
# root. Databricks Repos sets a notebook's cwd to its own directory, so we
# derive src/ and the repo root from there to make imports and config paths
# work regardless of where the repo is checked out.
NOTEBOOK_DIR = Path.cwd()
SRC_DIR = NOTEBOOK_DIR.parent.parent
REPO_ROOT = SRC_DIR.parent

sys.path.append(str(SRC_DIR))

from staging.extract import ingest_table_to_volume
from utils.api_client import APIClient
from utils.config_manager import get_config

# COMMAND ----------

dbutils.widgets.text("table_name", "")

table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

api_key = dbutils.secrets.get(scope="flatpay-case", key="api-key")

# COMMAND ----------

settings = get_config(str(REPO_ROOT / "configs" / "settings.yml"))
table = get_config(str(REPO_ROOT / "configs" / "staging" / "tables.yml"))["tables"][table_name]

base_url = settings["api"]["base_url"]
limit = settings["api"]["limit"]
max_requests_per_minute = settings["api"]["max_requests_per_minute"]

# COMMAND ----------

client = APIClient(
    base_url=base_url,
    api_key=api_key,
    max_requests_per_minute=max_requests_per_minute,
)

# COMMAND ----------

ingest_table_to_volume(
    client=client,
    endpoint=table["endpoint"],
    staging_dir=table["staging_path"],
    limit=limit,
)