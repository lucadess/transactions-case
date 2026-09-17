from abc import ABC, abstractmethod
from typing import List

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


class Writer(ABC):
    def __init__(self, spark: SparkSession, table: str, mode: str = "overwrite"):
        self.spark = spark
        self.table = table
        self.mode = mode

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass


class DeltaTableWriter(Writer):
    def write(self, df: DataFrame) -> None:
        self._ensure_table_exists(df)
        df.write.format("delta").mode(self.mode).option("overwriteSchema", "true").saveAsTable(self.table)

    def _ensure_table_exists(self, df: DataFrame) -> None:
        if not self.spark.catalog.tableExists(self.table):
            empty_df = self.spark.createDataFrame([], df.schema)
            empty_df.write.format("delta").saveAsTable(self.table)
