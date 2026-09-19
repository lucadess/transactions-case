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
    (2, "M2", "YYEcom", "FR", "2026-01-02T00:00:00", True, "FR456"),
    (3, "M3", "AcqCo", "ES", "2026-01-03T00:00:00", True, "ES789"),
    (4, "M4", "AcqCo", "NL", "2026-01-04T00:00:00", False, "NL321"),
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
    ("u2", "p2", 50.0, "EUR", "M2", "00", 2, 1, "T2", "2026-01-01T11:00:00"),
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
