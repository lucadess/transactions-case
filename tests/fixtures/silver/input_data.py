from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("merchant_id", StringType()),
        StructField("acquire", StringType()),
        StructField("country", StringType()),
        StructField("created", StringType()),
        StructField("test_merchant", BooleanType()),
        StructField("vat_number", StringType()),
    ]
)

CUSTOMERS_DATA = [
    (1, "M1", "AcqCo", "DE", "2026-01-01T00:00:00", False, "DE123"),
    (1, "M1", "AcqCo", "DE", "2026-01-01T00:00:00", False, "DE123"),  # duplicate PK, should be deduped
    (2, "M2", "YYEcom", "FR", "2026-01-02T00:00:00", True, "FR456"),
    (None, "M3", "AcqCo", "ES", "2026-01-03T00:00:00", False, "ES789"),  # null PK, should be dropped
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
        StructField("timestamp", StringType()),
    ]
)

TRANSACTIONS_DATA = [
    ("u1", "p1", 100.0, "USD", "M1", "00", 2, 1, "T1", "2026-01-01T10:00:00"),
    ("u1", "p1", 100.0, "USD", "M1", "00", 2, 1, "T1", "2026-01-01T10:00:00"),  # duplicate PK, should be deduped
    ("u2", "p2", 50.0, "EUR", "M2", "00", 2, 1, "T2", "2026-01-01T11:00:00"),
    (None, "p3", 20.0, "USD", "M1", "05", 2, 1, "T1", "2026-01-01T12:00:00"),  # null PK, should be dropped
]

OPERATION_STATUS_CODE_SCHEMA = StructType(
    [
        StructField("status_code_id", LongType()),
        StructField("name", StringType()),
    ]
)

OPERATION_STATUS_CODE_DATA = [
    (2, "Approved"),
    (2, "Approved"),  # duplicate PK, should be deduped
    (4, "Declined"),
    (None, "Unknown"),  # null PK, should be dropped
]
