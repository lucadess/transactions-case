from abc import ABC, abstractmethod

from utils.writer import apply_schema, get_primary_key, get_writer


class GoldGenerator(ABC):
    def __init__(self, spark, table_config: dict, settings: dict):
        self.spark = spark
        self.table_config = table_config
        self.settings = settings

    @abstractmethod
    def transform(self):
        """Reads its own source tables and returns the gold-level DataFrame."""

    def load(self, df) -> None:
        columns = self.table_config["columns"]
        df = apply_schema(df, columns)
        primary_key = get_primary_key(columns)
        target_table = f"{self.settings['catalog']}.{self.settings['schemas']['gold']}.{self.table_config['table_name']}"

        writer = get_writer(
            self.spark,
            target_table,
            self.table_config["write_mode"],
            primary_key=primary_key,
            table_config=self.table_config,
        )
        writer.write(df)

    def generate(self) -> None:
        self.load(self.transform())

    def _silver_table(self, table_name: str) -> str:
        return f"{self.settings['catalog']}.{self.settings['schemas']['silver']}.{table_name}"
