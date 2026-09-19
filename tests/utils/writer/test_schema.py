from utils.writer.schema import _create_table_from_config, _escape, apply_schema, create_table, get_primary_key


def test_escape_strips_and_backslash_escapes_single_quotes():
    assert _escape("  hello  ") == "hello"
    assert _escape("it's fine") == "it\\'s fine"


def test_create_table_from_config_creates_table_with_declared_columns_and_comment(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    table_config = {
        "comment": "Customer data",
        "columns": [
            {"name": "customer_id", "datatype": "bigint", "description": "The id"},
            {"name": "country", "datatype": "string", "description": "The country"},
        ],
    }

    create_table(spark, table, table_config=table_config)

    result = spark.table(table)
    assert [f.name for f in result.schema] == ["customer_id", "country"]
    assert dict(result.dtypes) == {"customer_id": "bigint", "country": "string"}
    assert result.count() == 0


def test_create_table_from_config_is_idempotent(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    table_config = {
        "comment": "Customer data",
        "columns": [{"name": "customer_id", "datatype": "bigint", "description": "The id"}],
    }

    create_table(spark, table, table_config=table_config)
    create_table(spark, table, table_config=table_config)  # should not raise / not alter the table

    assert spark.catalog.tableExists(table)


def test_create_table_from_config_preserves_apostrophes_in_comments(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    table_config = {
        "comment": "It's a table",
        "columns": [{"name": "id", "datatype": "bigint", "description": "It's a column"}],
    }

    _create_table_from_config(spark, table, table_config)

    assert spark.catalog.tableExists(table)
    table_comment = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").filter("col_name = 'Comment'").collect()
    assert table_comment[0]["data_type"] == "It's a table"


def test_create_table_without_config_uses_dataframe_schema(spark, schema):
    table = f"spark_catalog.{schema}.customers"
    df = spark.createDataFrame([(1, "a")], ["id", "name"])

    create_table(spark, table, df=df)

    result = spark.table(table)
    assert [f.name for f in result.schema] == ["id", "name"]
    assert result.count() == 0


def test_apply_schema_selects_and_casts_declared_columns(spark):
    df = spark.createDataFrame([("1", "a", "extra")], ["id", "name", "unused"])
    columns = [
        {"name": "id", "datatype": "bigint"},
        {"name": "name", "datatype": "string"},
    ]

    result = apply_schema(df, columns)

    assert result.columns == ["id", "name"]
    assert dict(result.dtypes) == {"id": "bigint", "name": "string"}
    assert result.collect()[0]["id"] == 1


def test_get_primary_key_returns_only_tagged_columns():
    columns = [
        {"name": "id", "tags": ["primary_key"]},
        {"name": "name", "tags": []},
        {"name": "status_id", "tags": ["foreign_key"]},
    ]

    assert get_primary_key(columns) == ["id"]


def test_get_primary_key_returns_empty_list_when_no_tags():
    columns = [{"name": "id", "tags": []}]

    assert get_primary_key(columns) == []
