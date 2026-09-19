import pytest

from gold.factory import get_gold_generator
from gold.generators.transactions import TransactionsGenerator


def test_get_gold_generator_returns_transactions_generator(fake_repo, monkeypatch):
    table_config = {
        "tables": {"transactions": {"table_name": "transactions", "write_mode": "overwrite", "columns": []}}
    }
    settings = {"catalog": "c", "schemas": {"gold": "g", "silver": "s"}}

    repo_root = fake_repo("gold", table_config, settings)
    monkeypatch.setattr("gold.factory.REPO_ROOT", repo_root)

    generator = get_gold_generator(spark="FAKE_SPARK", table_name="transactions")

    assert isinstance(generator, TransactionsGenerator)
    assert generator.spark == "FAKE_SPARK"
    assert generator.settings == settings


def test_get_gold_generator_raises_for_unregistered_table(fake_repo, monkeypatch):
    table_config = {"tables": {"unknown": {"table_name": "unknown", "write_mode": "overwrite", "columns": []}}}
    settings = {"catalog": "c", "schemas": {}}

    repo_root = fake_repo("gold", table_config, settings)
    monkeypatch.setattr("gold.factory.REPO_ROOT", repo_root)

    with pytest.raises(ValueError, match="No gold generator registered for table: unknown"):
        get_gold_generator(spark="FAKE_SPARK", table_name="unknown")
