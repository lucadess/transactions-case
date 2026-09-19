from pathlib import Path

from utils.config_manager import get_config
from utils.writer import apply_schema, get_primary_key, get_writer

REPO_ROOT = Path(__file__).resolve().parents[2]


def clean_data(spark, table_name: str):
    table = get_config(str(REPO_ROOT / "configs" / "silver" / "tables.yml"))["tables"][table_name]
    settings = get_config(str(REPO_ROOT / "configs" / "settings.yml"))

    catalog = settings["catalog"]
    source_table = f"{catalog}.{settings['schemas']['bronze']}.{table['table_name']}"
    target_table = f"{catalog}.{settings['schemas']['silver']}.{table['table_name']}"

    df = spark.table(source_table)
    df = apply_schema(df, table["columns"])

    primary_key = get_primary_key(table["columns"])
    df = df.dropna(subset=primary_key)
    df = df.dropDuplicates(subset=primary_key)

    writer = get_writer(spark, target_table, table["write_mode"], primary_key=primary_key, table_config=table)
    writer.write(df)
