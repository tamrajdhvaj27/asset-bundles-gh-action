# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
## git action project

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql import Row
from datetime import datetime

# Define schema
sales_schema = StructType([
    StructField("OrderID", StringType(), False),
    StructField("OrderDate", DateType(), False),
    StructField("Region", StringType(), True),
    StructField("Product", StringType(), False),
    StructField("Category", StringType(), False),
    StructField("Price", DoubleType(), False),
    StructField("Quantity", IntegerType(), False)
])

# Extended sample data
sales_data = [
    ("ORD009", datetime(2022, 1, 10), "West", "iPhone 15", "Electronics", 1099.99, 3),
    ("ORD010", datetime(2022, 2, 18), "East", "MacBook Pro", "Electronics", 1999.99, 1),
    ("ORD011", datetime(2022, 3, 6), "South", "Ergonomic Chair", "Furniture", 179.99, 2),
    ("ORD012", datetime(2022, 4, 20), "North", "4K Monitor", "Electronics", 329.99, 2),
    ("ORD013", datetime(2022, 5, 27), "West", "Standing Desk", "Furniture", 499.99, 1),
    ("ORD014", datetime(2022, 6, 15), "East", "iPad Pro", "Electronics", 899.99, 2),
    ("ORD015", datetime(2022, 7, 23), "South", "Corner Bookshelf", "Furniture", 249.99, 3),
    ("ORD016", datetime(2022, 8, 12), "North", "Laser Printer", "Electronics", 199.99, 1),
    ("ORD001", datetime(2023, 1, 5), "North", "iPhone 14", "Electronics", 999.99, 2),
    ("ORD002", datetime(2023, 2, 19), "East", "MacBook Air", "Electronics", 1199.49, 1),
    ("ORD003", datetime(2023, 3, 15), "South", "Desk Chair", "Furniture", 149.99, 4),
    ("ORD004", datetime(2023, 3, 17), "North", "Monitor", "Electronics", 249.99, 2),
    ("ORD005", datetime(2023, 5, 30), "West", "Office Desk", "Furniture", 399.99, 1),
    ("ORD006", datetime(2023, 6, 12), "East", "iPad", "Electronics", 499.99, 3),
    ("ORD007", datetime(2023, 7, 20), "South", "Bookshelf", "Furniture", 199.99, 2),
    ("ORD008", datetime(2023, 8, 8), "North", "Printer", "Electronics", 149.49, 1),
    ("ORD017", datetime(2023, 9, 14), "West", "Gaming Laptop", "Electronics", 1499.99, 2),
    ("ORD018", datetime(2023, 10, 2), "South", "Office Chair", "Furniture", 129.99, 5),
    ("ORD019", datetime(2023, 11, 11), "East", "Smartwatch", "Electronics", 299.99, 4),
    ("ORD020", datetime(2023, 12, 5), "North", "Bookshelf", "Furniture", 219.99, 2),
    ("ORD021", datetime(2024, 1, 8), "West", "Wireless Headphones", "Electronics", 199.99, 6),
    ("ORD022", datetime(2024, 2, 20), "South", "Coffee Table", "Furniture", 349.99, 1),
    ("ORD023", datetime(2024, 3, 15), "East", "Tablet", "Electronics", 599.99, 3),
    ("ORD024", datetime(2024, 4, 10), "North", "Office Desk", "Furniture", 459.99, 2),
    ("ORD025", datetime(2024, 5, 25), "West", "Smart TV", "Electronics", 1299.99, 1),
    ("ORD026", datetime(2024, 6, 18), "South", "Recliner Sofa", "Furniture", 899.99, 1),
    ("ORD027", datetime(2024, 7, 22), "East", "Bluetooth Speaker", "Electronics", 149.99, 7),
    ("ORD028", datetime(2024, 8, 30), "North", "Dining Table", "Furniture", 699.99, 1),
]

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
# sales_df.show()

# COMMAND ----------

from pyspark.sql.functions import year, month, col, expr

sales_transformed = (
    sales_df.withColumn("Year",year(col("OrderDate")))
            .withColumn("Month",month(col("OrderDate")))
            .withColumn("SalesAmount",expr("Price * Quantity"))
)

# COMMAND ----------

sales_summary = (
    sales_transformed.groupBy("Region","Category")
                     .sum("SalesAmount")
                     .withColumnRenamed("Sum(SalesAmount)","TotalSales")
                     .orderBy("Region","Category")
)

# COMMAND ----------

sales_summary.display()

# COMMAND ----------

sales_transformed.write.mode("overwrite").saveAsTable("<uc-schema-table-names>")
