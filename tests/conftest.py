import uuid
from pathlib import Path

import pytest

from utils.config_manager import get_config

TEST_SETTINGS_PATH = Path(__file__).parent / "fixtures" / "settings.yml"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse_dir = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def schema(spark):
    """A fresh, isolated schema (in Spark's built-in spark_catalog) for one test."""
    schema_name = f"test_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS spark_catalog.{schema_name}")
    yield schema_name
    spark.sql(f"DROP SCHEMA IF EXISTS spark_catalog.{schema_name} CASCADE")


@pytest.fixture
def layer_schemas(spark):
    """One isolated schema per pipeline layer, for cross-layer (bronze->silver->gold) end-to-end tests.

    Schema names come from tests/fixtures/settings.yml (the real test settings file), suffixed with a
    per-test random id so concurrent/sequential test runs never collide.
    """
    test_settings = get_config(str(TEST_SETTINGS_PATH))
    suffix = uuid.uuid4().hex[:8]
    layers = {layer: f"test_{suffix}_{name}" for layer, name in test_settings["schemas"].items()}

    for name in layers.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS spark_catalog.{name}")
    yield layers
    for name in layers.values():
        spark.sql(f"DROP SCHEMA IF EXISTS spark_catalog.{name} CASCADE")


@pytest.fixture
def fake_repo(tmp_path):
    """Returns a function that writes configs/<layer>/tables.yml and configs/settings.yml into a temp
    directory shaped like the real repo root. Monkeypatching a module's REPO_ROOT at that directory
    means its unmodified get_config calls read these test-only files instead of the real configs/."""
    import yaml

    def _write(layer: str, table_config: dict, settings: dict) -> Path:
        configs_dir = tmp_path / "configs"
        (configs_dir / layer).mkdir(parents=True, exist_ok=True)
        (configs_dir / layer / "tables.yml").write_text(yaml.dump(table_config))
        (configs_dir / "settings.yml").write_text(yaml.dump(settings))
        return tmp_path

    return _write
