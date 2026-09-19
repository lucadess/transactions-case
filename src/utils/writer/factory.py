from typing import List, Optional

from pyspark.sql import SparkSession

from .writer import DeltaTableWriter, Writer


def get_writer(
    spark: SparkSession,
    table: str,
    write_mode: str,
    primary_key: Optional[List[str]] = None,
    table_config: Optional[dict] = None,
) -> Writer:
    return DeltaTableWriter(spark, table, mode=write_mode, table_config=table_config)
