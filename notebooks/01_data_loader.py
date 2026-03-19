# Install yfinance
%pip install yfinance --quiet
dbutils.library.restartPython()

# Cell 2: Imports & Config
import yfinance as yf
import pandas as pd
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *

# ── Config ───────────────────────────────────────
CATALOG    = "stock_analytics"
SCHEMA     = "gold"

# Top 10 S&P 500 stocks — finance domain, MSCI relevant
TICKERS = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "GOOGL",  # Alphabet
    "AMZN",   # Amazon
    "NVDA",   # Nvidia
    "META",   # Meta
    "TSLA",   # Tesla
    "BRK-B",  # Berkshire Hathaway
    "JPM",    # JPMorgan Chase
    "V"       # Visa
]

START_DATE = "2023-01-01"   # 2+ years of history
END_DATE   = date.today().strftime("%Y-%m-%d")

print(f"✅ Config loaded")
print(f"📅 Date range  : {START_DATE} → {END_DATE}")
print(f"🏢 Tickers     : {TICKERS}")
print(f"📊 Total stocks: {len(TICKERS)}")


# Fetch OHLCV data from yfinance
print("🚀 Fetching data from yfinance...\n")

all_data = []

for ticker in TICKERS:
    print(f"  📡 Fetching: {ticker}")
    try:
        stock = yf.Ticker(ticker)
        df    = stock.history(start=START_DATE, end=END_DATE)
        
        if df.empty:
            print(f"    ⚠️  No data for {ticker}")
            continue
        
        # Reset index to get Date as column
        df = df.reset_index()
        
        # Select and rename columns
        df["ticker"]      = ticker
        df["trade_date"]  = df["Date"].dt.date
        df["open"]        = df["Open"].round(6)
        df["high"]        = df["High"].round(6)
        df["low"]         = df["Low"].round(6)
        df["close"]       = df["Close"].round(6)
        df["volume"]      = df["Volume"].astype(float)
        df["data_source"] = "yfinance"
        
        # Keep only needed columns
        df = df[[
            "ticker", "trade_date", "open", "high",
            "low", "close", "volume", "data_source"
        ]]
        
        all_data.append(df)
        print(f"    ✅ {len(df)} rows fetched")
    
    except Exception as e:
        print(f"    ❌ Failed {ticker}: {str(e)}")

# Combine all tickers into one DataFrame
combined_df = pd.concat(all_data, ignore_index=True)
print(f"\n📊 Total rows fetched: {len(combined_df)}")
print(f"📅 Date range        : {combined_df['trade_date'].min()} → {combined_df['trade_date'].max()}")


#  Convert to Spark DataFrame + add derived columns

# ── Convert Pandas → Spark ──────────────────────
schema = StructType([
    StructField("ticker",      StringType(),  True),
    StructField("trade_date",  DateType(),    True),
    StructField("open",        DoubleType(),  True),
    StructField("high",        DoubleType(),  True),
    StructField("low",         DoubleType(),  True),
    StructField("close",       DoubleType(),  True),
    StructField("volume",      DoubleType(),  True),
    StructField("data_source", StringType(),  True)
])

spark_df = spark.createDataFrame(combined_df, schema=schema)

# ── Window for moving averages ───────────────────
window_7d  = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-6,  0)
window_30d = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-29, 0)

# ── Add derived columns ──────────────────────────
enriched_df = (
    spark_df
        .withColumn("daily_return_pct",
            F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 4))
        .withColumn("price_range",
            F.round(F.col("high") - F.col("low"), 4))
        .withColumn("moving_avg_7d",
            F.round(F.avg("close").over(window_7d), 4))
        .withColumn("moving_avg_30d",
            F.round(F.avg("close").over(window_30d), 4))
        .withColumn("ingested_at",
            F.current_timestamp())
)

print(f"✅ Spark DataFrame created")
print(f"   Rows    : {enriched_df.count()}")
print(f"   Columns : {len(enriched_df.columns)}")
enriched_df.printSchema()


# # Cell 5: Write to gold.daily_prices
# (
#     enriched_df
#         .select(
#             "ticker", "trade_date",
#             "open", "high", "low", "close", "volume",
#             "daily_return_pct", "price_range",
#             "moving_avg_7d", "moving_avg_30d",
#             "data_source", "ingested_at"
#         )
#         .write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable("stock_analytics.gold.daily_prices")
# )

# count = spark.table("stock_analytics.gold.daily_prices").count()
# print(f"✅ Written to stock_analytics.gold.daily_prices")
# print(f"   Total rows: {count}")


# # Compute + write stock_volatility
# window_rank = Window.orderBy(F.desc("volatility_stddev"))

# volatility_df = (
#     enriched_df
#         .groupBy("ticker")
#         .agg(
#             F.round(F.stddev("daily_return_pct"), 4) .alias("volatility_stddev"),
#             F.round(F.avg("daily_return_pct"),    4) .alias("avg_daily_return"),
#             F.round(F.max("daily_return_pct"),    4) .alias("best_day_return"),
#             F.round(F.min("daily_return_pct"),    4) .alias("worst_day_return"),
#             F.count("*")                             .alias("total_trading_days"),
#             F.min("trade_date")                      .alias("data_from"),
#             F.max("trade_date")                      .alias("data_to")
#         )
#         .withColumn("risk_tier",
#             F.when(F.col("volatility_stddev") > 3,  "🔴 High")
#              .when(F.col("volatility_stddev") > 1.5, "🟡 Medium")
#              .otherwise(                             "🟢 Low")
#         )
# )

# (
#     volatility_df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable("stock_analytics.gold.stock_volatility")
# )

# print("✅ Written to stock_analytics.gold.stock_volatility")
# display(volatility_df.orderBy("volatility_stddev", ascending=False))



# # Compute + write stock_volatility
# window_rank = Window.orderBy(F.desc("volatility_stddev"))

# volatility_df = (
#     enriched_df
#         .groupBy("ticker")
#         .agg(
#             F.round(F.stddev("daily_return_pct"), 4) .alias("volatility_stddev"),
#             F.round(F.avg("daily_return_pct"),    4) .alias("avg_daily_return"),
#             F.round(F.max("daily_return_pct"),    4) .alias("best_day_return"),
#             F.round(F.min("daily_return_pct"),    4) .alias("worst_day_return"),
#             F.count("*")                             .alias("total_trading_days"),
#             F.min("trade_date")                      .alias("data_from"),
#             F.max("trade_date")                      .alias("data_to")
#         )
#         .withColumn("risk_tier",
#             F.when(F.col("volatility_stddev") > 3,  "🔴 High")
#              .when(F.col("volatility_stddev") > 1.5, "🟡 Medium")
#              .otherwise(                             "🟢 Low")
#         )
# )

# (
#     volatility_df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable("stock_analytics.gold.stock_volatility")
# )

# print("✅ Written to stock_analytics.gold.stock_volatility")
# display(volatility_df.orderBy("volatility_stddev", ascending=False))

# # Cell 7: Compute + write monthly_performance
window_month_rank = Window.partitionBy("month").orderBy(F.desc("monthly_return_pct"))

monthly_df = (
    enriched_df
        .withColumn("month", F.date_format("trade_date", "yyyy-MM"))
        .groupBy("ticker", "month")
        .agg(
            F.round(F.sum("daily_return_pct"),  2).alias("monthly_return_pct"),
            F.round(F.max("daily_return_pct"),  2).alias("best_single_day"),
            F.round(F.min("daily_return_pct"),  2).alias("worst_single_day"),
            F.round(F.avg("close"),             4).alias("avg_close"),
            F.count("*")                          .alias("trading_days")
        )
        .withColumn("rank", F.rank().over(window_month_rank))
)

(
    monthly_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("stock_analytics.gold.monthly_performance")
)

print("✅ Written to stock_analytics.gold.monthly_performance")
print(f"   Total rows: {monthly_df.count()}")
display(monthly_df.filter(F.col("rank") == 1).orderBy("month"))

# Preview monthly winners
print("\n🏆 Monthly winners (rank=1):")
display(
    spark.table("stock_analytics.gold.monthly_performance")
        .filter(F.col("rank") == 1)
        .orderBy("month")
)

#  Verify all 3 Gold tables
print("=" * 55)
print("📋 FINAL VERIFICATION — All Gold Tables")
print("=" * 55)

tables = {
    "daily_prices"       : "stock_analytics.gold.daily_prices",
    "stock_volatility"   : "stock_analytics.gold.stock_volatility",
    "monthly_performance": "stock_analytics.gold.monthly_performance"
}

for name, table in tables.items():
    count = spark.table(table).count()
    print(f"\n✅ {name}")
    print(f"   Rows: {count}")
    spark.table(table).show(3, truncate=False)

print("\n🎉 Data loader complete — ready to build the App!")
































