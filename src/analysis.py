from pyspark.sql import SparkSession, functions as F

# chemins vus depuis les conteneurs Docker
DATA_PATH = "/opt/spark-data/players_data_light-2024_2025.csv"
OUTPUT_PATH = "/opt/spark-output/player_workload"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PlayerWorkloadAndInjuryRisk")
        .getOrCreate()
    )


def main():
    spark = build_spark_session()

    print("🔹 Loading dataset...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(DATA_PATH)
    )

    print(f"Dataset loaded: {df.count()} rows, {len(df.columns)} columns")

    # Colonnes qu'on va utiliser (adaptées à ton CSV light)
    cols_needed = [
        "Player", "Nation", "Pos", "Squad", "Comp",
        "Age", "MP", "Min", "90s",
        # défense
        "Tkl", "Blocks_stats_defense", "Int", "Clr",
        # possession / courses
        "Carries", "PrgC_stats_possession", "PrgR_stats_possession",
        # pertes de balle / duels
        "Mis", "Dis",
    ]

    # avertir si certaines colonnes manquent
    for c in cols_needed:
        if c not in df.columns:
            print(f"⚠ WARNING: column {c} not found in dataset")

    df = df.select(*[c for c in cols_needed if c in df.columns])

    # Filtrer joueurs avec au moins 5 x 90 minutes
    df = df.filter(F.col("90s") >= 5)

    # Remplacer les nulls par 0 pour les stats numériques
    num_cols = [
        "MP", "Min", "90s",
        "Tkl", "Blocks_stats_defense", "Int", "Clr",
        "Carries", "PrgC_stats_possession", "PrgR_stats_possession",
        "Mis", "Dis",
    ]
    for c in num_cols:
        if c in df.columns:
            df = df.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))

    # --- Features de charge de travail ---

    # minutes par match (intensité d'utilisation)
    df = df.withColumn(
        "minutes_per_game",
        F.when(F.col("MP") > 0, F.col("Min") / F.col("MP")).otherwise(F.lit(0.0))
    )

    # taux d'utilisation théorique (0 à ~1)
    df = df.withColumn(
        "utilisation_ratio",
        F.when(F.col("90s") > 0, F.col("Min") / (F.col("90s") * F.lit(90.0))).otherwise(F.lit(0.0))
    )

    # charge défensive par 90 minutes
    df = df.withColumn(
        "defensive_load",
        (F.col("Tkl") +
         F.col("Blocks_stats_defense") +
         F.col("Int") +
         F.col("Clr")) / F.col("90s")
    )

    # charge de course / intensité de déplacement
    df = df.withColumn(
        "running_load",
        (F.col("Carries") +
         F.col("PrgC_stats_possession") +
         F.col("PrgR_stats_possession")) / F.col("90s")
    )

    # charge liée aux duels / pertes de balle
    df = df.withColumn(
        "duel_load",
        (F.col("Mis") + F.col("Dis")) / F.col("90s")
    )

    # index global de workload (pondérations à expliquer dans le README)
    df = df.withColumn(
        "workload_index",
        0.4 * F.col("utilisation_ratio") * 10  # on remet à une échelle comparable
        + 0.35 * F.col("defensive_load")
        + 0.25 * F.col("running_load")
    )

    # Classement final
    result = (
        df.select(
            "Player", "Nation", "Pos", "Squad", "Comp",
            "Age", "MP", "Min", "90s",
            "minutes_per_game", "utilisation_ratio",
            "defensive_load", "running_load", "duel_load",
            "workload_index",
        )
        .orderBy(F.col("workload_index").desc())
    )

    print("🔹 Top 20 players by workload_index:")
    result.show(20, truncate=False)

    # Écriture des résultats dans output/
    (
        result
        .coalesce(1)
        .write
        .option("header", True)
        .mode("overwrite")
        .csv(OUTPUT_PATH)
    )

    print(f"✅ Results written to {OUTPUT_PATH}")
    spark.stop()


if _name_ == "_main_":
    main()