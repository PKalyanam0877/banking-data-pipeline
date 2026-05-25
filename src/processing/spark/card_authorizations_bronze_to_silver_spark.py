import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--s3-access-key", required=True)
    parser.add_argument("--s3-secret-key", required=True)
    return parser.parse_args()


def configure_s3(spark, args):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", args.s3_endpoint)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", args.s3_access_key)
    hadoop_conf.set("fs.s3a.secret.key", args.s3_secret_key)


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("card-authorizations-bronze-to-silver-spark")
        .getOrCreate()
    )

    configure_s3(spark, args)

    bronze_df = spark.read.option("multiLine", "true").json(args.input_path)

    print("Bronze schema")
    bronze_df.printSchema()

    print("Bronze count")
    print(bronze_df.count())

    silver_df = bronze_df.select(
        col("parsed_value.event_id").alias("event_id"),
        col("parsed_value.transaction_id").alias("transaction_id"),
        col("parsed_value.authorization_id").alias("authorization_id"),
        col("parsed_value.customer_id").alias("customer_id"),
        col("parsed_value.account_id").alias("account_id"),
        col("parsed_value.card_id").alias("card_id"),
        col("parsed_value.event_time").alias("event_time"),
        col("parsed_value.amount").cast("decimal(18,2)").alias("amount"),
        col("parsed_value.currency").alias("currency"),
        col("parsed_value.merchant_id").alias("merchant_id"),
        col("parsed_value.merchant_name").alias("merchant_name"),
        col("parsed_value.merchant_category_code").alias("merchant_category_code"),
        col("parsed_value.merchant_country").alias("merchant_country"),
        col("parsed_value.merchant_city").alias("merchant_city"),
        col("parsed_value.authorization_status").alias("authorization_status"),
        col("parsed_value.channel").alias("channel"),
        col("parsed_value.card_present").alias("card_present"),
        col("parsed_value.entry_mode").alias("entry_mode"),
        col("parsed_value.source_system").alias("source_system"),
        col("parsed_value.schema_version").alias("schema_version"),
        col("kafka_topic").alias("bronze_kafka_topic"),
        col("kafka_partition").alias("bronze_kafka_partition"),
        col("kafka_offset").alias("bronze_kafka_offset"),
        col("bronze_ingest_time"),
        current_timestamp().alias("silver_processed_time"),
    )

    required_columns = [
        "event_id",
        "transaction_id",
        "authorization_id",
        "customer_id",
        "account_id",
        "card_id",
        "event_time",
        "amount",
        "currency",
        "merchant_id",
        "merchant_name",
        "merchant_category_code",
        "merchant_country",
        "authorization_status",
        "channel",
        "card_present",
        "entry_mode",
        "source_system",
        "schema_version",
        "bronze_kafka_topic",
        "bronze_kafka_partition",
        "bronze_kafka_offset",
        "bronze_ingest_time",
    ]

    valid_condition = (
        col("amount").isNotNull()
        & (col("amount") >= 0)
        & (col("currency") == "USD")
        & col("authorization_status").isin("approved", "declined")
    )

    for column_name in required_columns:
        valid_condition = valid_condition & col(column_name).isNotNull()

    valid_df = silver_df.filter(valid_condition)
    invalid_df = silver_df.filter(~valid_condition)

    deduped_df = valid_df.dropDuplicates(["event_id"])

    raw_candidate_count = silver_df.count()
    valid_candidate_count = valid_df.count()
    invalid_candidate_count = invalid_df.count()
    deduped_candidate_count = deduped_df.count()
    duplicate_valid_event_id_count = valid_candidate_count - deduped_candidate_count

    print("Raw Silver candidate count")
    print(raw_candidate_count)

    print("Valid Silver candidate count before dedupe")
    print(valid_candidate_count)

    print("Invalid Silver candidate count before dedupe")
    print(invalid_candidate_count)

    print("Valid Silver candidate count after dedupe")
    print(deduped_candidate_count)

    print("Duplicate valid event_id count")
    print(duplicate_valid_event_id_count)

    print("Silver candidate schema")
    silver_df.printSchema()

    print("Silver candidate sample")
    silver_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
