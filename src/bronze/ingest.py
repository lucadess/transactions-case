from pathlib import Path

from pyspark.sql import functions as F

from utils.config_manager import get_config
from utils.reader import get_reader
from utils.writer import get_writer

REPO_ROOT = Path(__file__).resolve().parents[2]


def ingest_data(spark, table_name: str):
    table = get_config(str(REPO_ROOT / "configs" / "bronze" / "tables.yml"))["tables"][table_name]
    settings = get_config(str(REPO_ROOT / "configs" / "settings.yml"))

    source_path = table["source_path"]
    write_mode = table["write_mode"]
    target_table = f"{settings['catalog']}.{settings['schemas']['bronze']}.{table['table_name']}"

    reader = get_reader(spark, source_path)
    df = reader.read(spark)
    df = df.withColumn("ingestion_date", F.current_timestamp())

    writer = get_writer(spark, target_table, write_mode)
    writer.write(df)