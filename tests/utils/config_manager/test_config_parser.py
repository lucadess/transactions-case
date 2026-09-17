import pytest

from utils.config_manager import get_config, get_layer_config


def test_get_layer_config_bronze_has_expected_tables():
    config = get_layer_config("bronze")
    assert "transactions" in config["tables"]
    assert "merchants" in config["tables"]


def test_get_layer_config_silver_has_schema_and_primary_key():
    config = get_layer_config("silver")
    transactions = config["tables"]["transactions"]
    assert transactions["primary_key"] == ["transaction_id"]
    assert any(col["name"] == "transaction_id" for col in transactions["schema"])


def test_get_layer_config_gold_has_expected_tables():
    config = get_layer_config("gold")
    assert set(config["tables"].keys()) == {"dim_merchants", "dim_date", "fact_transaction"}


def test_get_layer_config_missing_layer_raises():
    with pytest.raises(FileNotFoundError):
        get_layer_config("nonexistent_layer")


def test_get_config_schemas():
    config = get_config("schemas")
    assert config["catalog"] == "flatpay_case"
    assert config["api"]["base_url"] == "https://dataengineercaseapi-production.up.railway.app/"
    assert config["api"]["limit"] == 1000
    assert config["schemas"]["bronze"] == "bronze"
    assert config["schemas"]["silver"] == "silver"
    assert config["schemas"]["gold"] == "gold"


def test_get_config_missing_raises():
    with pytest.raises(FileNotFoundError):
        get_config("nonexistent_config")
