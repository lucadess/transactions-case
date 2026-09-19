from pathlib import Path

import pytest
from pyspark.testing import assertDataFrameEqual

from bronze.ingest import explode_json_array, ingest_data
from utils.config_manager import get_config

from fixtures.bronze import expected_data

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "bronze"

TABLE_CASES = [
    pytest.param("customers", expected_data.CUSTOMERS_SCHEMA, expected_data.CUSTOMERS_DATA, id="customers"),
    pytest.param("transactions", expected_data.TRANSACTIONS_SCHEMA, expected_data.TRANSACTIONS_DATA, id="transactions"),
    pytest.param(
        "operation_status_code",
        expected_data.OPERATION_STATUS_CODE_SCHEMA,
        expected_data.OPERATION_STATUS_CODE_DATA,
        id="operation_status_code",
    ),
]


def test_get_correct_bronze_config():
    config = get_config("configs/bronze/tables.yml")

    assert set(config["tables"].keys()) == {"customers", "transactions", "operation_status_code"}
    customers = config["tables"]["customers"]
    assert customers["table_name"] == "customers"
    assert customers["source_path"] == "/Volumes/flatpay_case/staging/uploads/customers/"
    assert customers["write_mode"] == "overwrite"


def test_explode_json_array_flattens_items_and_drops_pagination_fields(spark):
    page_path = FIXTURES / "input" / "customers" / "page_0000000.json"
    df = spark.read.json(str(page_path), multiLine=True)

    result = explode_json_array(df)

    assert set(result.columns) == {
        "customer_id",
        "merchant_id",
        "acquire",
        "country",
        "created",
        "test_merchant",
        "vat_number",
    }
    assert result.count() == 2

    rows_by_id = {row["customer_id"]: row.asDict() for row in result.collect()}
    assert rows_by_id[1]["merchant_id"] == "M1"
    assert rows_by_id[2]["acquire"] == "YYEcom"


@pytest.mark.parametrize("table_name, expected_schema, expected_rows", TABLE_CASES)
def test_ingest_data_end_to_end_matches_expected_fixture(
    spark, layer_schemas, fake_repo, monkeypatch, table_name, expected_schema, expected_rows
):
    settings = get_config(str(FIXTURES.parent / "settings.yml"))
    settings["schemas"] = layer_schemas

    bronze_config = get_config("configs/bronze/tables.yml")
    bronze_config["tables"][table_name]["source_path"] = str(FIXTURES / "input" / table_name)

    repo_root = fake_repo("bronze", bronze_config, settings)
    monkeypatch.setattr("bronze.ingest.REPO_ROOT", repo_root)

    ingest_data(spark, table_name)

    target_table = f"spark_catalog.{layer_schemas['bronze']}.{table_name}"
    result_df = spark.table(target_table)
    expected_df = spark.createDataFrame(expected_rows, expected_schema)

    assertDataFrameEqual(result_df, expected_df, ignoreColumnOrder=True)
