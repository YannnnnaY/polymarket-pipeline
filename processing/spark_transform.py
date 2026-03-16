from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os

def create_spark_session():
    spark = SparkSession.builder \
        .appName("PolymarketPipeline") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def load_raw(spark, path="data/raw_markets.parquet"):
    df = spark.read.parquet(path)
    print(f"Loaded {df.count()} rows")
    df.printSchema()
    return df

def transform(df):
    # Clean and cast
    df = df.withColumn("volume", F.col("volume").cast(DoubleType())) \
           .withColumn("volume_24hr", F.col("volume_24hr").cast(DoubleType())) \
           .withColumn("liquidity", F.col("liquidity").cast(DoubleType())) \
           .withColumn("end_date", F.to_timestamp("end_date")) \
           .withColumn("start_date", F.to_timestamp("start_date"))

    # Add derived columns
    df = df.withColumn(
            "days_until_close",
            F.datediff(F.col("end_date"), F.current_date())
        ) \
        .withColumn(
            "volume_to_liquidity_ratio",
            F.when(F.col("liquidity") > 0,
                F.round(F.col("volume") / F.col("liquidity"), 4)
            ).otherwise(None)
        ) \
        .withColumn(
            "is_high_volume",
            F.col("volume_24hr") > 10000
        )

    return df

def aggregate_by_category(df):
    return df.groupBy("category") \
        .agg(
            F.count("market_id").alias("total_markets"),
            F.round(F.sum("volume"), 2).alias("total_volume"),
            F.round(F.sum("volume_24hr"), 2).alias("total_volume_24hr"),
            F.round(F.avg("liquidity"), 2).alias("avg_liquidity"),
            F.round(F.avg("volume_to_liquidity_ratio"), 4).alias("avg_vol_liq_ratio")
        ) \
        .orderBy(F.col("total_volume").desc())

def save_outputs(df_clean, df_category):
    df_clean.write.mode("overwrite").parquet("data/processed_markets.parquet")
    df_category.write.mode("overwrite").parquet("data/category_summary.parquet")
    print("Saved processed_markets.parquet")
    print("Saved category_summary.parquet")

if __name__ == "__main__":
    spark = create_spark_session()
    df_raw = load_raw(spark)
    df_clean = transform(df_raw)
    df_category = aggregate_by_category(df_clean)

    print("\nCategory Summary:")
    df_category.show(truncate=False)

    save_outputs(df_clean, df_category)
    spark.stop()
    print("Done!")
