from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def flatten_df(df: DataFrame, separator: str = "_") -> DataFrame:
    while True:
        struct_fields = [f for f in df.schema.fields if isinstance(f.dataType, StructType)]
        if not struct_fields:
            return df

        select_cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                for nested_field in field.dataType.fields:
                    select_cols.append(
                        F.col(f"{field.name}.{nested_field.name}").alias(
                            f"{field.name}{separator}{nested_field.name}"
                        )
                    )
            else:
                select_cols.append(F.col(field.name))

        df = df.select(*select_cols)

class Reader(ABC):
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path

    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        pass


class CSVReader(Reader):
    def __init__(self, spark: SparkSession, path: str, header: bool = True, inferSchema: bool = True):
        super().__init__(spark, path)
        self.header = header
        self.inferSchema = inferSchema

    def read(self, spark: SparkSession) -> DataFrame:
        return spark.read.csv(self.path, header=self.header, inferSchema=self.inferSchema)


class JSONReader(Reader):
    def __init__(self, spark: SparkSession, path: str, multiLine: bool = True):
        super().__init__(spark, path)
        self.multiLine = multiLine

    def read(self, spark: SparkSession) -> DataFrame:
        df = spark.read.json(self.path, multiLine=self.multiLine)
        return flatten_df(df)