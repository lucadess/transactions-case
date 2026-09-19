from pathlib import Path

import pytest
from pyspark.testing import assertDataFrameEqual

from silver.clean import clean_data
from utils.config_manager import get_config

from fixtures.silver import expected_data, input_data

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "silver"

TABLE_CASES = [
    pytest.param(
        "customers",
        input_data.CUSTOMERS_SCHEMA,
        input_data.CUSTOMERS_DATA,
        expected_data.CUSTOMERS_SCHEMA,
        expected_data.CUSTOMERS_DATA,
        id="customers",
    ),
    pytest.param(
        "transactions",
        input_data.TRANSACTIONS_SCHEMA,
        input_data.TRANSACTIONS_DATA,
        expected_data.TRANSACTIONS_SCHEMA,
        expected_data.TRANSACTIONS_DATA,
        id="transactions",
    ),
    pytest.param(
        "operation_status_code",
        input_data.OPERATION_STATUS_CODE_SCHEMA,
        input_data.OPERATION_STATUS_CODE_DATA,
        expected_data.OPERATION_STATUS_CODE_SCHEMA,
        expected_data.OPERATION_STATUS_CODE_DATA,
        id="operation_status_code",
    ),
]


def test_get_correct_silver_config():
    config = get_config("configs/silver/tables.yml")

    assert set(config["tables"].keys()) == {"customers", "transactions", "operation_status_code"}
    customers = config["tables"]["customers"]
    assert customers["table_name"] == "customers"
    assert customers["write_mode"] == "overwrite"
    pk_columns = [c["name"] for c in customers["columns"] if "primary_key" in c["tags"]]
    assert pk_columns == ["customer_id"]


def test_dropna_and_dropduplicates_on_primary_key(spark):
    df = spark.createDataFrame(
        [(1, "a"), (1, "a"), (2, "b"), (None, "c")],
        ["id", "name"],
    )
    primary_key = ["id"]

    result = df.dropna(subset=primary_key).dropDuplicates(subset=primary_key)

    assert sorted(row["id"] for row in result.collect()) == [1, 2]


@pytest.mark.parametrize("table_name, bronze_schema, bronze_data, expected_schema, expected_rows", TABLE_CASES)
def test_clean_data_end_to_end_matches_expected_fixture(
    spark, layer_schemas, fake_repo, monkeypatch, table_name, bronze_schema, bronze_data, expected_schema, expected_rows
):
    input_df = spark.createDataFrame(bronze_data, bronze_schema)
    bronze_table = f"spark_catalog.{layer_schemas['bronze']}.{table_name}"
    input_df.write.format("delta").saveAsTable(bronze_table)

    settings = get_config(str(FIXTURES.parent / "settings.yml"))
    settings["schemas"] = layer_schemas

    silver_config = get_config("configs/silver/tables.yml")

    repo_root = fake_repo("silver", silver_config, settings)
    monkeypatch.setattr("silver.clean.REPO_ROOT", repo_root)

    clean_data(spark, table_name)

    target_table = f"spark_catalog.{layer_schemas['silver']}.{table_name}"
    result_df = spark.table(target_table)
    expected_df = spark.createDataFrame(expected_rows, expected_schema)

    assertDataFrameEqual(result_df, expected_df)
