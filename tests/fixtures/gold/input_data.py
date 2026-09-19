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

# u1: M1, non-ECOM, non-test, status=2, result="00" -> kept
# u2: M2 is ECOM ("YYEcom") -> excluded
# u3: result code "05" (not "00") -> excluded
# u4: status code 4 (not 2) -> excluded
# u5: M3 is a test merchant -> excluded
TRANSACTIONS_DATA = [
    ("u1", "p1", 100.0, "USD", "M1", "00", 2, 1, "T1", datetime(2026, 1, 1, 10, 0, 0)),
    ("u2", "p2", 50.0, "EUR", "M2", "00", 2, 1, "T2", datetime(2026, 1, 1, 11, 0, 0)),
    ("u3", "p3", 20.0, "USD", "M1", "05", 2, 1, "T1", datetime(2026, 1, 1, 12, 0, 0)),
    ("u4", "p4", 30.0, "USD", "M2", "00", 4, 1, "T3", datetime(2026, 1, 1, 13, 0, 0)),
    ("u5", "p5", 40.0, "USD", "M3", "00", 2, 1, "T4", datetime(2026, 1, 1, 14, 0, 0)),
]

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
    (1, "M1", "AcqCo", "DE", datetime(2026, 1, 1), False, "DE123"),
    (2, "M2", "YYEcom", "FR", datetime(2026, 1, 1), False, "FR456"),
    (3, "M3", "AcqCo", "ES", datetime(2026, 1, 1), True, "ES789"),
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
