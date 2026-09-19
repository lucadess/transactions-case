import os
from pyspark.sql import SparkSession
from .readers import JSONReader, Reader


def get_reader(spark: SparkSession, source_path: str) -> Reader:
    readers = {
        "json": JSONReader,
    }

    # Staging source paths are UC volume directories of page files (no
    # extension of their own), so they always read as JSON.
    if os.path.isdir(source_path):
        return JSONReader(spark, source_path)

    file_type = os.path.splitext(source_path)[1].lower().lstrip(".")

    if file_type not in readers:
        raise ValueError(f"Unsupported file type: {file_type}")
    return readers[file_type](spark, source_path)