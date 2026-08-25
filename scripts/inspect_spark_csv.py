from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

columns = [
    "order_id",
    "customer_id",
    "customer_email",
    "phone",
    "order_date",
    "items",
    "price",
    "quantity",
    "shipping_cost",
    "total",
    "status",
]
spark = SparkSession.builder.master("local[2]").appName("inspect-csv").getOrCreate()
try:
    schema = StructType([StructField(column, StringType(), True) for column in columns])
    frame = (
        spark.read.option("header", True)
        .option("escape", '"')
        .schema(schema)
        .csv("data/sample_orders.csv")
    )
    for row in frame.take(3):
        print(row.asDict())
finally:
    spark.stop()
