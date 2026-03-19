# Cell 1: Create Catalog & Schemas
spark.sql("CREATE CATALOG IF NOT EXISTS stock_analytics")
spark.sql("USE CATALOG stock_analytics")

schemas = ["bronze", "silver", "gold"]
for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS stock_analytics.{schema}")
    print(f"✅ Schema created: stock_analytics.{schema}")
