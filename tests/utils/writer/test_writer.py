import pytest

from utils.writer import DeltaTableWriter, get_writer
from utils.writer.writer import Writer


def test_get_writer_returns_delta_table_writer_with_correct_attrs():
    table_config = {"comment": "x", "columns": []}
    writer = get_writer(
        spark="FAKE_SPARK",
        table="cat.schema.table",
        write_mode="overwrite",
        primary_key=["id"],
        table_config=table_config,
    )

    assert isinstance(writer, Writer)
    assert isinstance(writer, DeltaTableWriter)
    assert writer.spark == "FAKE_SPARK"
    assert writer.table == "cat.schema.table"
    assert writer.mode == "overwrite"
    assert writer.table_config is table_config


def test_ensure_table_exists_creates_table_from_df_when_missing(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    writer = DeltaTableWriter(spark, table)
    df = spark.createDataFrame([(1, "a")], ["id", "name"])

    assert not spark.catalog.tableExists(table)
    writer._ensure_table_exists(df)
    assert spark.catalog.tableExists(table)
    assert spark.table(table).count() == 0


def test_ensure_table_exists_skips_when_table_already_present(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    df.write.format("delta").saveAsTable(table)
    df.write.format("delta").mode("append").saveAsTable(table)  # give it a row to prove it's untouched

    writer = DeltaTableWriter(spark, table)
    writer._ensure_table_exists(df)

    assert spark.table(table).count() == 2


def test_validate_schema_passes_when_matching(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    df.write.format("delta").saveAsTable(table)

    writer = DeltaTableWriter(spark, table)
    writer._validate_schema(df)  # should not raise


def test_validate_schema_raises_on_mismatch(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    original_df = spark.createDataFrame([(1, "a")], ["id", "name"])
    original_df.write.format("delta").saveAsTable(table)

    mismatched_df = spark.createDataFrame([(1, "a", "extra")], ["id", "name", "extra_col"])
    writer = DeltaTableWriter(spark, table)

    with pytest.raises(ValueError, match="Schema mismatch"):
        writer._validate_schema(mismatched_df)


def test_write_creates_table_and_writes_rows_end_to_end(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    writer = DeltaTableWriter(spark, table)
    writer.write(df)

    result = spark.table(table)
    assert result.count() == 2
    assert {row["id"] for row in result.collect()} == {1, 2}


def test_write_overwrite_mode_replaces_existing_rows(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    first_df = spark.createDataFrame([(1, "a")], ["id", "name"])
    DeltaTableWriter(spark, table, mode="overwrite").write(first_df)

    second_df = spark.createDataFrame([(2, "b")], ["id", "name"])
    DeltaTableWriter(spark, table, mode="overwrite").write(second_df)

    result = spark.table(table)
    assert result.count() == 1
    assert result.collect()[0]["id"] == 2


def test_write_raises_on_schema_mismatch_against_existing_table(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    DeltaTableWriter(spark, table).write(spark.createDataFrame([(1, "a")], ["id", "name"]))

    mismatched_df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(ValueError, match="Schema mismatch"):
        DeltaTableWriter(spark, table).write(mismatched_df)


def test_write_creates_table_from_config_when_table_config_given(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    table_config = {
        "comment": "Customers",
        "columns": [
            {"name": "id", "datatype": "bigint", "description": "id"},
            {"name": "name", "datatype": "string", "description": "name"},
        ],
    }
    df = spark.createDataFrame([(1, "a")], ["id", "name"])

    writer = DeltaTableWriter(spark, table, table_config=table_config)
    writer.write(df)

    result = spark.table(table)
    assert dict(result.dtypes) == {"id": "bigint", "name": "string"}
    assert result.count() == 1
