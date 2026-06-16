"""
DataFlow360 — Main ETL Pipeline
Orchestrates ingestion, transformation, and loading of
customer and sales data into the data warehouse.
"""

import logging
import boto3
import pandas as pd
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

S3_BUCKET       = "dataflow360-raw"
S3_PREFIX       = "incoming/"
REDSHIFT_HOST   = "your-redshift-cluster.us-east-1.redshift.amazonaws.com"
REDSHIFT_DB     = "datawarehouse"
REDSHIFT_USER   = "etl_user"
REDSHIFT_PORT   = 5439
GLUE_DATABASE   = "dataflow360_catalog"

def get_spark_session():
    spark = (
        SparkSession.builder
        .appName("DataFlow360-ETL")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized")
    return spark

def list_s3_files(bucket, prefix):
    s3 = boto3.client("s3")
    today = datetime.utcnow().strftime("%Y/%m/%d")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}{today}/")
    files = [obj["Key"] for obj in response.get("Contents", [])]
    logger.info(f"Found {len(files)} files in S3 for date {today}")
    return files

def ingest_from_s3(spark, bucket, files):
    if not files:
        raise ValueError("No input files found in S3 for today's batch.")
    paths = [f"s3://{bucket}/{f}" for f in files]
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(paths)
    logger.info(f"Ingested {df.count():,} raw records from S3")
    return df

def run_transformations(spark, raw_df):
    from transformations.pyspark_jobs.customer_sales_transform import CustomerSalesTransformer
    transformer = CustomerSalesTransformer(spark)
    transformed_df = transformer.run(raw_df)
    logger.info(f"Transformation complete. Output records: {transformed_df.count():,}")
    return transformed_df

def run_validation(df):
    checks_passed = True
    critical_cols = ["customer_id", "transaction_date", "amount"]
    for col in critical_cols:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            logger.warning(f"NULL check FAILED: {null_count} nulls in '{col}'")
            checks_passed = False
        else:
            logger.info(f"NULL check PASSED: '{col}'")
    total = df.count()
    distinct = df.dropDuplicates(["customer_id", "transaction_date"]).count()
    if total - distinct > 0:
        logger.warning(f"DUPLICATE check FAILED: {total - distinct} duplicates")
        checks_passed = False
    return checks_passed

def load_to_redshift(df, table, password):
    jdbc_url = (
        f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"
        f"?user={REDSHIFT_USER}&password={password}"
    )
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("driver", "com.amazon.redshift.jdbc42.Driver")
        .mode("append")
        .save()
    )
    logger.info(f"Loaded {df.count():,} records into Redshift: {table}")

def run_pipeline(redshift_password, snowflake_password):
    logger.info("=== DataFlow360 ETL Pipeline Started ===")
    spark = get_spark_session()
    try:
        files = list_s3_files(S3_BUCKET, S3_PREFIX)
        raw_df = ingest_from_s3(spark, S3_BUCKET, files)
        transformed_df = run_transformations(spark, raw_df)
        if not run_validation(transformed_df):
            raise ValueError("Data quality validation failed. Aborting.")
        load_to_redshift(transformed_df, "public.customer_sales_fact", redshift_password)
        logger.info("=== DataFlow360 ETL Pipeline Completed Successfully ===")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    import os
    run_pipeline(
        redshift_password=os.environ["REDSHIFT_PASSWORD"],
        snowflake_password=os.environ["SNOWFLAKE_PASSWORD"],
    )
