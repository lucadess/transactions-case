from typing import List, Optional

from pyspark.sql import SparkSession

from .writer import DeltaTableWriter, MergeWriter, Writer


def get_writer(
    spark: SparkSession,
    table: str,
    write_mode: str,
    primary_key: Optional[List[str]] = None,
) -> Writer:
    if write_mode == "merge":
        if not primary_key:
            raise ValueError("primary_key is required for write_mode 'merge'")
        return MergeWriter(spark, table, primary_key)

    return DeltaTableWriter(spark, table, mode=write_mode)
