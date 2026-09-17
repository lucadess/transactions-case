import os
from pyspark.sql import SparkSession
from .readers import CSVReader, JSONReader, Reader


def get_reader(spark: SparkSession, source_path: str) -> Reader:
    readers = {
        "csv": CSVReader,
        "json": JSONReader,
    }

    file_type = os.path.splitext(source_path)[1].lower().lstrip(".")

    if file_type not in readers:
        raise ValueError(f"Unsupported file type: {file_type}")
    return readers[file_type](spark, source_path)