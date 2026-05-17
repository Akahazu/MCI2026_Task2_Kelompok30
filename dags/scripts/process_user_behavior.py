from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob


def run_user_behavior():

    spark = SparkSession.builder \
        .appName("Customer_Ranking") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("📥 Membaca parquet...")

    df = spark.read.parquet(
        "file:///opt/airflow/data_lake/orders/"
    )

    print("📊 Menghitung customer behavior...")


    customer_df = df.groupBy("user_id").agg(
        F.count("*").alias("basket_size"),
        F.sum("reordered").alias("reorder_total"),
        (F.sum("reordered").cast("float") / F.count("*")).alias("reorder_rate"),
        F.avg("days_since_prior_order").alias("days_since_prior_order"),
        F.avg("order_hour").alias("avg_order_hour")
    )

    # MIN MAX NORMALIZATION
    # basket min max
    basket_stats = customer_df.agg(
        F.min("basket_size").alias("min_basket"),
        F.max("basket_size").alias("max_basket")
    ).collect()[0]

    min_basket = basket_stats["min_basket"]
    max_basket = basket_stats["max_basket"]

    # recency min max
    recency_stats = customer_df.agg(
        F.min("days_since_prior_order").alias("min_days"),
        F.max("days_since_prior_order").alias("max_days")
    ).collect()[0]

    min_days = recency_stats["min_days"]
    max_days = recency_stats["max_days"]

    # BASKET SCORE
    if max_basket == min_basket:
        customer_df = customer_df.withColumn(
            "basket_score",
            F.lit(1.0)
        )

    else:
        customer_df = customer_df.withColumn(
            "basket_score",
            (F.col("basket_size")- min_basket) / (max_basket- min_basket)
        )

    # RECENCY SCORE
    if max_days == min_days:

        customer_df = customer_df.withColumn(
            "recency_score",
            F.lit(1.0)
        )

    else:

        customer_df = customer_df.withColumn(
            "recency_score",
            1 - ((F.col("days_since_prior_order") - min_days) / (max_days - min_days))
        )

    # FINAL SCORE

    customer_df = customer_df.withColumn(
        "final_score",
        (
            (F.col("reorder_rate") * 0.5) +
            (F.col("basket_score") * 0.3) +
            (F.col("recency_score") * 0.2)
        ) * 100
    )

    # SORTING

    final_df = customer_df.orderBy(
        F.desc("final_score")
    ).toPandas()

    spark.stop()

    print(f"Memuat {len(final_df)} user ke ClickHouse...")

    client = Client(
        host="clickhouse-server",
        user="admin",
        password="rahasia"
    )

    client.execute("CREATE DATABASE IF NOT EXISTS analytics")

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.customer_ranking (
            user_id Int32,
            basket_size Int32,
            reorder_total Int32,
            days_since_prior_order Float32,
            avg_order_hour Float32,
            basket_score Float32,
            reorder_rate Float32,
            recency_score Float32,
            final_score Float32
        )ENGINE = MergeTree()
        ORDER BY final_score
    """)

    # refresh snapshot
    client.execute("TRUNCATE TABLE analytics.customer_ranking")

    data = []

    for _, row in final_df.iterrows():

        data.append((
            int(row["user_id"]),
            int(row["basket_size"]),
            int(row["reorder_total"]),
            float(row["days_since_prior_order"]),
            float(row["avg_order_hour"]),
            float(row["basket_score"]),
            float(row["reorder_rate"]),
            float(row["recency_score"]),
            float(row["final_score"])
        ))

    if data:
        client.execute("INSERT INTO analytics.customer_ranking VALUES", data)

    print("🧹 Membersihkan parquet lama dari data lake...")
    files = glob.glob("/opt/airflow/data_lake/orders/*.parquet")
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Gagal hapus {f}: {e.strerror}")

    print(f"✅ Pipeline selesai. {len(data)} user tersimpan di ClickHouse.")


if __name__ == "__main__":
    run_user_behavior()