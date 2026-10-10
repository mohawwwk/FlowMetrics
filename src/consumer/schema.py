from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def ecommerce_schema() -> StructType:
    return StructType([
        StructField("InvoiceNo", StringType(), nullable=True),
        StructField("StockCode", StringType(), nullable=True),
        StructField("Description", StringType(), nullable=True),
        StructField("Quantity", IntegerType(), nullable=True),
        StructField("InvoiceDate", StringType(), nullable=True),
        StructField("UnitPrice", StringType(), nullable=True),
        StructField("CustomerID", StringType(), nullable=True),
        StructField("Country", StringType(), nullable=True),
        StructField("_corrupt_record", StringType(), nullable=True),
    ])
