from datetime import datetime

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("uuid", StringType()),
        StructField("payment_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("amount_eur", DoubleType()),
        StructField("operation_result_code", StringType()),
        StructField("operation_status_code", LongType()),
        StructField("operation_status_name", StringType()),
        StructField("provider_code", LongType()),
        StructField("terminal_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("country", StringType()),
    ]
)

# Only u1 survives the filters (see fixtures/gold/input_data.py for why).
# fetch_eur_rates is mocked in the test to {"USD": 0.9, "EUR": 1.0}, so 100.0 USD -> 90.0 EUR.
TRANSACTIONS_DATA = [
    ("u1", "p1", "M1", 100.0, "USD", 90.0, "00", 2, "Approved", 1, "T1", datetime(2026, 1, 1, 10, 0, 0), "DE"),
]
