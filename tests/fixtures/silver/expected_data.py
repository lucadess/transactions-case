from datetime import datetime

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("merchant_id", StringType()),
        StructField("acquire", StringType()),
        StructField("country", StringType()),
        StructField("created", TimestampType()),
        StructField("test_merchant", BooleanType()),
        StructField("vat_number", StringType()),
    ]
)

CUSTOMERS_DATA = [
    (1, "M1", "AcqCo", "DE", datetime(2026, 1, 1, 0, 0, 0), False, "DE123"),
    (2, "M2", "YYEcom", "FR", datetime(2026, 1, 2, 0, 0, 0), True, "FR456"),
]

TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("uuid", StringType()),
        StructField("payment_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("merchant_id", StringType()),
        StructField("operation_result_code", StringType()),
        StructField("operation_status_code", LongType()),
        StructField("provider_code", LongType()),
        StructField("terminal_id", StringType()),
        StructField("timestamp", TimestampType()),
    ]
)

TRANSACTIONS_DATA = [
    ("u1", "p1", 100.0, "USD", "M1", "00", 2, 1, "T1", datetime(2026, 1, 1, 10, 0, 0)),
    ("u2", "p2", 50.0, "EUR", "M2", "00", 2, 1, "T2", datetime(2026, 1, 1, 11, 0, 0)),
]

OPERATION_STATUS_CODE_SCHEMA = StructType(
    [
        StructField("status_code_id", LongType()),
        StructField("name", StringType()),
    ]
)

OPERATION_STATUS_CODE_DATA = [
    (2, "Approved"),
    (4, "Declined"),
]
