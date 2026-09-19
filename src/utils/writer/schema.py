from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def create_table(
    spark: SparkSession,
    table: str,
    df: Optional[DataFrame] = None,
    table_config: Optional[dict] = None,
) -> None:
    if table_config is not None:
        _create_table_from_config(spark, table, table_config)
    else:
        empty_df = spark.createDataFrame([], df.schema)
        empty_df.write.format("delta").saveAsTable(table)


def _create_table_from_config(spark: SparkSession, table: str, table_config: dict) -> None:
    columns_ddl = ", ".join(
        f"{col['name']} {col['datatype']} COMMENT '{_escape(col['description'])}'"
        for col in table_config["columns"]
    )
    comment = _escape(table_config["comment"])

    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({columns_ddl}) USING DELTA COMMENT '{comment}'")


def _escape(text: str) -> str:
    return text.strip().replace("'", "\\'")


def apply_schema(df: DataFrame, columns) -> DataFrame:
    """Selects and casts each source column to the datatype declared for it in the table config."""
    return df.select(*[df[col["name"]].cast(col["datatype"]).alias(col["name"]) for col in columns])


def get_primary_key(columns):
    return [col["name"] for col in columns if "primary_key" in col["tags"]]
