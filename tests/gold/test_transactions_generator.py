from pathlib import Path

from pyspark.testing import assertDataFrameEqual

from gold.factory import get_gold_generator
from utils.config_manager import get_config

from fixtures.gold.expected_data import TRANSACTIONS_DATA as EXPECTED_DATA, TRANSACTIONS_SCHEMA as EXPECTED_SCHEMA
from fixtures.gold.input_data import (
    CUSTOMERS_DATA,
    CUSTOMERS_SCHEMA,
    OPERATION_STATUS_CODE_DATA,
    OPERATION_STATUS_CODE_SCHEMA,
    TRANSACTIONS_DATA as INPUT_TRANSACTIONS_DATA,
    TRANSACTIONS_SCHEMA as INPUT_TRANSACTIONS_SCHEMA,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "gold"


def test_transactions_generator_end_to_end_matches_expected_fixture(spark, layer_schemas, fake_repo, monkeypatch):
    spark.createDataFrame(INPUT_TRANSACTIONS_DATA, INPUT_TRANSACTIONS_SCHEMA).write.format("delta").saveAsTable(
        f"spark_catalog.{layer_schemas['silver']}.transactions"
    )
    spark.createDataFrame(CUSTOMERS_DATA, CUSTOMERS_SCHEMA).write.format("delta").saveAsTable(
        f"spark_catalog.{layer_schemas['silver']}.customers"
    )
    spark.createDataFrame(OPERATION_STATUS_CODE_DATA, OPERATION_STATUS_CODE_SCHEMA).write.format("delta").saveAsTable(
        f"spark_catalog.{layer_schemas['silver']}.operation_status_code"
    )

    settings = get_config(str(FIXTURES.parent / "settings.yml"))
    settings["schemas"] = layer_schemas

    gold_config = get_config("configs/gold/tables.yml")

    repo_root = fake_repo("gold", gold_config, settings)
    monkeypatch.setattr("gold.factory.REPO_ROOT", repo_root)
    monkeypatch.setattr("gold.generators.transactions.fetch_eur_rates", lambda: {"USD": 0.9, "EUR": 1.0})

    generator = get_gold_generator(spark, "transactions")
    generator.generate()

    target_table = f"spark_catalog.{layer_schemas['gold']}.transactions"
    result_df = spark.table(target_table)
    expected_df = spark.createDataFrame(EXPECTED_DATA, EXPECTED_SCHEMA)

    assertDataFrameEqual(result_df, expected_df)
