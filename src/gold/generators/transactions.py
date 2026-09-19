from pyspark.sql import functions as F

from gold.fx_rates import fetch_eur_rates
from gold.gold_generators import GoldGenerator


class TransactionsGenerator(GoldGenerator):
    def get_transactions(self):
        return self.spark.table(self._silver_table("transactions"))

    def get_customers(self):
        return self.spark.table(self._silver_table("customers"))

    def get_operation_status_code(self):
        return self.spark.table(self._silver_table("operation_status_code")).withColumnRenamed(
            "name", "operation_status_name"
        )

    def transform(self):
        transactions = self.get_transactions()
        customers = self.get_customers()
        status = self.get_operation_status_code()

        df = transactions.join(customers, on="merchant_id", how="inner")
        df = df.join(status, df["operation_status_code"] == status["status_code_id"], how="inner")

        df = df.filter(
            (F.col("operation_status_code") == 2)
            & (F.col("operation_result_code") == "00")
            & (F.col("acquire") != "YYEcom")
            & (~F.col("test_merchant"))
        )

        rates_df = self.spark.createDataFrame(list(fetch_eur_rates().items()), ["currency", "eur_rate"])
        df = df.join(rates_df, on="currency", how="inner")
        df = df.withColumn("amount_eur", F.col("amount") * F.col("eur_rate"))
        return df.withColumnRenamed("timestamp", "transaction_timestamp")
