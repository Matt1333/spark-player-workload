from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    regexp_replace,
    split,
    coalesce,
    row_number,
    avg,
    count,
)
from pyspark.sql.window import Window


DATA_PATH = "/opt/spark-data/players_data_light-2024_2025.csv"
OUTPUT_PATH = "/opt/spark-output/player_workload"


def to_double(df, col_name):
    """
    Utility: cast a column to double safely.
    If the column does not exist, we just return the df unchanged.
    """
    if col_name not in df.columns:
        return df
    return df.withColumn(
        col_name,
        regexp_replace(col(col_name).cast("string"), ",", ".").cast("double"),
    )


def main():
    spark = (
        SparkSession.builder
        .appName("PlayerWorkloadAnalysis")
        .getOrCreate()
    )

    # To avoid generating too many small files
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    print("🔹 Loading dataset...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(DATA_PATH)
    )

    print(f"Dataset loaded: {df.count()} rows, {len(df.columns)} columns")

    # Ensure this columns exists
    numeric_cols = [
        "MP",
        "Min",
        "90s",
        "Tkl+Int",
        "Carries",
        "PrgDist_stats_possession",
        "Fls",
    ]

    for c in numeric_cols:
        df = to_double(df, c)

    # Filter: only players with a minimum of game time
    df_filtered = df.filter(col("90s") >= 5)

    

    # Mean of minutes played 
    df_metrics = df_filtered.withColumn(
        "minutes_per_game",
        when(col("MP") > 0, col("Min") / col("MP")).otherwise(lit(0.0)),
    )

    # Player played games 
    df_metrics = df_metrics.withColumn(
        "utilisation_ratio",
        when(col("90s") > 0, col("Min") / (col("90s") * lit(90.0))).otherwise(lit(0.0)),
    )

    # defensive 1v1 by 90min
    df_metrics = df_metrics.withColumn(
        "defensive_load",
        when(col("90s") > 0, coalesce(col("Tkl+Int"), lit(0.0)) / col("90s")).otherwise(
            lit(0.0)
        ),
    )

    # distance run by 90 minutes
    df_metrics = df_metrics.withColumn(
        "running_load",
        when(
            col("90s") > 0,
            coalesce(col("PrgDist_stats_possession"), lit(0.0)) / col("90s"),
        ).otherwise(lit(0.0)),
    )

    # fool made every 90 minutes
    df_metrics = df_metrics.withColumn(
        "duel_load",
        when(col("90s") > 0, coalesce(col("Fls"), lit(0.0)) / col("90s")).otherwise(
            lit(0.0)
        ),
    )

    # Workload index formula based on mutliple factors and index 
    df_metrics = df_metrics.withColumn(
        "workload_index",
        0.4 * col("running_load")
        + 0.4 * col("defensive_load")
        + 0.2 * col("duel_load"),
    )
    #forula to have the injury risk = workload * games played * minutes played by games (90)
    df_metrics = df_metrics.withColumn(
        "injury_risk_score",
        col("workload_index")
        * col("utilisation_ratio")
        * (col("minutes_per_game") / lit(90.0)),
    )
    # Post filter 
    df_metrics = df_metrics.withColumn(
    "Pos_main",
    when(col("Pos").contains("GK"), "GK")
    .when(col("Pos").contains("DF"), "DF")
    .when(col("Pos").contains("MF"), "MF")
    .when(col("Pos").contains("FW"), "FW")
    .otherwise("OTHER")
)
    top20 = (
        df_metrics.select(
            "Player",
            "Nation",
            "Pos",
            "Squad",
            "Comp",
            "Age",
            "MP",
            "Min",
            "90s",
            "minutes_per_game",
            "utilisation_ratio",
            "defensive_load",
            "running_load",
            "duel_load",
            "workload_index",
            "injury_risk_score",
            "Pos_main",
        )
        .orderBy(col("workload_index").desc())
        .limit(20)
    )

    print("🔹 Top 20 players by workload_index:")
    top20.show(truncate=False)

    w_pos = Window.partitionBy("Pos_main").orderBy(col("workload_index").desc())

    top_by_position = (
        df_metrics
        .withColumn("rank_in_pos", row_number().over(w_pos))
        .filter(col("rank_in_pos") <= 10)
        .select(
            "Pos_main",
            "rank_in_pos",
            "Player",
            "Squad",
            "Comp",
            "MP",
            "Min",
            "90s",
            "workload_index",
            "injury_risk_score",
        )
        .orderBy("Pos_main", "rank_in_pos")
    )

    print("🔹 Top 10 players by workload_index for each main position:")
    top_by_position.show(truncate=False)

    team_summary = (
        df_metrics
        .groupBy("Squad", "Comp")
        .agg(
            avg("workload_index").alias("avg_workload_index"),
            avg("injury_risk_score").alias("avg_injury_risk_score"),
            count("*").alias("num_players"),
        )
        .orderBy(col("avg_workload_index").desc())
    )
    print("🔹 Team-level summary (highest average workload):")
    team_summary.show(20, truncate=False)
    print(f"💾 Writing results under: {OUTPUT_PATH}")

    # Top 20 global
    top20.coalesce(1).write.mode("overwrite").option("header", True).csv(
        OUTPUT_PATH + "/overall_top20"
    )

    #Top 10 player/post 
    top_by_position.coalesce(1).write.mode("overwrite").option("header", True).csv(
        OUTPUT_PATH + "/top_by_position"
    )
    team_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        OUTPUT_PATH + "/team_summary"
    )
    df_metrics.coalesce(1).write.mode("overwrite").option("header", True).csv(
        OUTPUT_PATH + "/full_player_workload"
    )

    print("All results written successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
