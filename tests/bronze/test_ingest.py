import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.testing import assertDataFrameEqual

from bronze.ingest import explode_json_array, ingest_data
from staging.extract import ingest_table_to_volume
from utils.api_client import APIClient
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


# --- staging.extract.ingest_table_to_volume (resumable page-fetching) ---


def _page(offset: int, count: int, limit: int, total: int) -> dict:
    return {
        "items": [{"id": offset + i} for i in range(count)],
        "limit": limit,
        "offset": offset,
        "total": total,
    }


def _write_page(staging_dir: str, offset: int, count: int, limit: int, total: int) -> None:
    path = os.path.join(staging_dir, f"page_{offset:07d}.json")
    with open(path, "w") as f:
        json.dump(_page(offset, count, limit, total), f)


def test_pagination_stops_when_offset_reaches_total(tmp_path):
    limit = 10
    total = 20
    client = MagicMock(spec=APIClient)
    client.fetch_page.side_effect = [
        _page(0, 10, limit, total),
        _page(10, 10, limit, total),
    ]

    ingest_table_to_volume(client, "/v1/customers", str(tmp_path), limit=limit)

    assert client.fetch_page.call_count == 2
    client.fetch_page.assert_any_call("/v1/customers", limit=limit, offset=0)
    client.fetch_page.assert_any_call("/v1/customers", limit=limit, offset=10)


def test_pagination_stops_when_page_returns_fewer_than_limit(tmp_path):
    limit = 10
    misleading_total = 1_000_000
    client = MagicMock(spec=APIClient)
    client.fetch_page.side_effect = [
        _page(0, 10, limit, misleading_total),
        _page(10, 3, limit, misleading_total),
    ]

    ingest_table_to_volume(client, "/v1/customers", str(tmp_path), limit=limit)

    assert client.fetch_page.call_count == 2


def test_existing_page_file_is_skipped_on_rerun(tmp_path):
    limit = 10
    total = 25
    _write_page(str(tmp_path), 0, 10, limit, total)
    _write_page(str(tmp_path), 10, 10, limit, total)

    client = MagicMock(spec=APIClient)
    client.fetch_page.side_effect = [_page(20, 5, limit, total)]

    ingest_table_to_volume(client, "/v1/customers", str(tmp_path), limit=limit)

    client.fetch_page.assert_called_once_with("/v1/customers", limit=limit, offset=20)
    assert os.path.exists(os.path.join(str(tmp_path), "page_0000020.json"))


def test_fully_staged_run_makes_zero_new_api_calls(tmp_path):
    limit = 10
    total = 25
    client = MagicMock(spec=APIClient)
    client.fetch_page.side_effect = [
        _page(0, 10, limit, total),
        _page(10, 10, limit, total),
        _page(20, 5, limit, total),
    ]

    ingest_table_to_volume(client, "/v1/customers", str(tmp_path), limit=limit)
    assert client.fetch_page.call_count == 3

    ingest_table_to_volume(client, "/v1/customers", str(tmp_path), limit=limit)
    assert client.fetch_page.call_count == 3


def test_leftover_tmp_file_is_not_treated_as_completed(tmp_path):
    limit = 10
    total = 10
    staging_dir = str(tmp_path)

    # Simulate a crash mid-write: a .tmp file exists but the real page file does not.
    tmp_path_file = os.path.join(staging_dir, "page_0000000.json.tmp")
    with open(tmp_path_file, "w") as f:
        f.write("{corrupted, incomplete")

    client = MagicMock(spec=APIClient)
    client.fetch_page.side_effect = [_page(0, 10, limit, total)]

    ingest_table_to_volume(client, "/v1/customers", staging_dir, limit=limit)

    client.fetch_page.assert_called_once_with("/v1/customers", limit=limit, offset=0)
    assert os.path.exists(os.path.join(staging_dir, "page_0000000.json"))


def test_no_stray_tmp_files_remain_after_run(tmp_path):
    limit = 10
    total = 10
    staging_dir = str(tmp_path)

    client = MagicMock(spec=APIClient)
    client.fetch_page.side_effect = [_page(0, 10, limit, total)]

    ingest_table_to_volume(client, "/v1/customers", staging_dir, limit=limit)

    files = os.listdir(staging_dir)
    assert "page_0000000.json" in files
    assert not any(f.endswith(".tmp") for f in files)


def test_page_written_via_atomic_rename(tmp_path):
    limit = 10
    total = 5
    staging_dir = str(tmp_path)

    client = MagicMock(spec=APIClient)
    client.fetch_page.side_effect = [_page(0, 5, limit, total)]

    ingest_table_to_volume(client, "/v1/customers", staging_dir, limit=limit)

    page_path = os.path.join(staging_dir, "page_0000000.json")
    with open(page_path) as f:
        content = json.load(f)

    assert content == _page(0, 5, limit, total)
    assert not os.path.exists(page_path + ".tmp")


# --- bronze.ingest.ingest_data / explode_json_array ---


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
