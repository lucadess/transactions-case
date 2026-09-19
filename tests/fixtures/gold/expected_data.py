from datetime import datetime
from decimal import Decimal

from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType, TimestampType

TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("uuid", StringType()),
        StructField("payment_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("amount", DecimalType(18, 2)),
        StructField("currency", StringType()),
        StructField("amount_eur", DecimalType(18, 2)),
        StructField("operation_result_code", StringType()),
        StructField("operation_status_code", LongType()),
        StructField("operation_status_name", StringType()),
        StructField("provider_code", LongType()),
        StructField("terminal_id", StringType()),
        StructField("transaction_timestamp", TimestampType()),
    ]
)

# Only u1 survives the filters (see fixtures/gold/input_data.py for why).
# fetch_eur_rates is mocked in the test to {"USD": 0.9, "EUR": 1.0}, so 100.57 USD -> 90.513 EUR,
# which the decimal(18,2) cast in load() rounds to 90.51.
TRANSACTIONS_DATA = [
    (
        "u1",
        "p1",
        "M1",
        Decimal("100.57"),
        "USD",
        Decimal("90.51"),
        "00",
        2,
        "Approved",
        1,
        "T1",
        datetime(2026, 1, 1, 10, 0, 0),
    ),
]
