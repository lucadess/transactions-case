import os
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame


class Reader(ABC):
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path

    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        pass

class JSONReader(Reader):
    def __init__(self, spark: SparkSession, path: str, multiLine: bool = True):
        super().__init__(spark, path)
        self.multiLine = multiLine

    def read(self, spark: SparkSession) -> DataFrame:
        pages = os.path.join(self.path, "*.json")
        return spark.read.json(pages, multiLine=self.multiLine)