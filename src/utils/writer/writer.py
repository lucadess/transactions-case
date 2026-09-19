from abc import ABC, abstractmethod
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from .schema import create_table


class Writer(ABC):
    def __init__(self, spark: SparkSession, table: str, mode: str = "overwrite"):
        self.spark = spark
        self.table = table
        self.mode = mode

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass


class DeltaTableWriter(Writer):
    def __init__(
        self,
        spark: SparkSession,
        table: str,
        mode: str = "overwrite",
        table_config: Optional[dict] = None,
    ):
        super().__init__(spark, table, mode)
        self.table_config = table_config

    def write(self, df: DataFrame) -> None:
        self._ensure_table_exists(df)
        self._validate_schema(df)
        df.write.format("delta").mode(self.mode).saveAsTable(self.table)

    def _ensure_table_exists(self, df: DataFrame) -> None:
        if not self.spark.catalog.tableExists(self.table):
            create_table(self.spark, self.table, df=df, table_config=self.table_config)

    def _validate_schema(self, df: DataFrame) -> None:
        existing_schema = self.spark.table(self.table).schema
        expected = [(field.name, field.dataType) for field in existing_schema]
        actual = [(field.name, field.dataType) for field in df.schema]

        if actual != expected:
            raise ValueError(
                f"Schema mismatch for table {self.table}: "
                f"expected {existing_schema.simpleString()}, got {df.schema.simpleString()}"
            )
