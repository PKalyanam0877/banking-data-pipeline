import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-path",
        required=True,
        help="Input Bronze JSON path. Example: /opt/spark/work-dir/tmp/sample_bronze_card.json",
    )
    parser.add_argument(
        "--s3-endpoint",
        default=None,
        help="Optional S3-compatible endpoint for MinIO, such as http://minio:9000.",
    )
    parser.add_argument(
        "--s3-access-key",
        default=None,
        help="Optional S3 access key.",
    )
    parser.add_argument(
        "--s3-secret-key",
        default=None,
        help="Optional S3 secret key.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("read-bronze-card-authorizations")
        .getOrCreate()
    )

    if args.s3_endpoint:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", args.s3_endpoint)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        if args.s3_access_key:
            hadoop_conf.set("fs.s3a.access.key", args.s3_access_key)
        if args.s3_secret_key:
            hadoop_conf.set("fs.s3a.secret.key", args.s3_secret_key)

    bronze_df = spark.read.option("multiLine", "true").json(args.input_path)

    print("Bronze schema")
    bronze_df.printSchema()

    print("Bronze record count")
    print(bronze_df.count())

    card_df = bronze_df.select(
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("bronze_ingest_time"),
        col("parsed_value.event_id").alias("event_id"),
        col("parsed_value.transaction_id").alias("transaction_id"),
        col("parsed_value.customer_id").alias("customer_id"),
        col("parsed_value.account_id").alias("account_id"),
        col("parsed_value.card_id").alias("card_id"),
        col("parsed_value.event_time").alias("event_time"),
        col("parsed_value.amount").alias("amount"),
        col("parsed_value.currency").alias("currency"),
        col("parsed_value.merchant_name").alias("merchant_name"),
        col("parsed_value.merchant_country").alias("merchant_country"),
        col("parsed_value.authorization_status").alias("authorization_status"),
        col("parsed_value.channel").alias("channel"),
    )

    print("Flattened card authorization schema")
    card_df.printSchema()

    print("Flattened card authorization sample")
    card_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
