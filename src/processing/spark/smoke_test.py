from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def main():
    spark = (
        SparkSession.builder
        .appName("banking-platform-spark-smoke-test")
        .getOrCreate()
    )

    rows = [
        ("spark_test", "card_authorization"),
        ("spark_test", "login_event"),
        ("spark_test", "fraud_risk"),
    ]

    dataframe = spark.createDataFrame(rows, ["job", "event_domain"])
    result = dataframe.groupBy("job").agg(count("*").alias("count"))

    print("Spark smoke test result")
    result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
