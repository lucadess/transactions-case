from pathlib import Path

from gold.generators.transactions import TransactionsGenerator
from gold.gold_generators import GoldGenerator
from utils.config_manager import get_config

REPO_ROOT = Path(__file__).resolve().parents[2]

GENERATORS = {
    "transactions": TransactionsGenerator,
}


def get_gold_generator(spark, table_name: str) -> GoldGenerator:
    table_config = get_config(str(REPO_ROOT / "configs" / "gold" / "tables.yml"))["tables"][table_name]
    settings = get_config(str(REPO_ROOT / "configs" / "settings.yml"))

    if table_name not in GENERATORS:
        raise ValueError(f"No gold generator registered for table: {table_name}")

    return GENERATORS[table_name](spark, table_config, settings)
