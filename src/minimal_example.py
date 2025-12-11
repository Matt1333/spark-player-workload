from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("MinimalSparkExample")
        .getOrCreate()
    )

    data = [("Matt", 1), ("Erwan", 2), ("Teacher", 3)]
    df = spark.createDataFrame(data, ["name", "value"])

    print("Spark session is running, tiny DataFrame below:")
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
