"""
DataFlow360 — PySpark Transformation Job
Cleans, deduplicates, standardizes, and enriches
customer and sales data for warehouse loading.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType, StringType, LongType
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


class CustomerSalesTransformer:

    def __init__(self, spark):
        self.spark = spark

    def run(self, raw_df):
        logger.info(f"Starting transformations. Input: {raw_df.count():,}")
        df = raw_df
        df = self.cast_schema(df)
        df = self.clean_data(df)
        df = self.deduplicate(df)
        df = self.standardize_fields(df)
        df = self.enrich(df)
        df = self.add_audit_columns(df)
        logger.info(f"Transformations complete. Output: {df.count():,}")
        return df

    def cast_schema(self, df):
        return (
            df
            .withColumn("customer_id", F.col("customer_id").cast(LongType()))
            .withColumn("transaction_date", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
            .withColumn("amount", F.col("amount").cast(DoubleType()))
            .withColumn("quantity", F.col("quantity").cast(LongType()))
            .withColumn("product_id", F.col("product_id").cast(LongType()))
        )

    def clean_data(self, df):
        initial = df.count()
        df = (
            df
            .dropna(subset=["customer_id", "transaction_date", "amount"])
            .filter(F.col("amount") > 0)
            .filter(F.col("amount") <= 1_000_000)
            .filter(F.col("transaction_date") <= F.current_date())
            .withColumn("customer_name", F.trim(F.col("customer_name")))
            .withColumn("product_name", F.trim(F.col("product_name")))
            .withColumn("region", F.trim(F.upper(F.col("region"))))
        )
        dropped = initial - df.count()
        logger.info(f"Cleaned {dropped:,} invalid records")
        return df

    def deduplicate(self, df):
        window = Window.partitionBy(
            "customer_id", "transaction_date", "product_id"
        ).orderBy(F.col("ingestion_timestamp").desc())
        return (
            df
            .withColumn("row_num", F.row_number().over(window))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

    def standardize_fields(self, df):
        return (
            df
            .withColumn("customer_segment",
                F.when(F.col("amount") >= 10000, "Enterprise")
                 .when(F.col("amount") >= 1000, "Mid-Market")
                 .otherwise("SMB")
            )
            .withColumn("transaction_year", F.year(F.col("transaction_date")))
            .withColumn("transaction_month", F.month(F.col("transaction_date")))
            .withColumn("transaction_quarter",
                F.concat(
                    F.year(F.col("transaction_date")).cast(StringType()),
                    F.lit("-Q"),
                    F.quarter(F.col("transaction_date")).cast(StringType())
                )
            )
        )

    def enrich(self, df):
        window = Window.partitionBy("customer_id").orderBy("transaction_date")
        return (
            df
            .withColumn("revenue", F.col("amount") * F.col("quantity"))
            .withColumn("cumulative_revenue", F.sum("revenue").over(window))
            .withColumn("avg_transaction_value", F.avg("amount").over(window))
            .withColumn("transaction_rank", F.rank().over(window))
        )

    def add_audit_columns(self, df):
        return (
            df
            .withColumn("batch_date", F.current_date())
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("pipeline_name", F.lit("DataFlow360"))
            .withColumn("pipeline_version", F.lit("1.0.0"))
        )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataFlow360-Transform").getOrCreate()
    raw_df = spark.read.parquet("s3://dataflow360-raw/staging/latest/")
    transformer = CustomerSalesTransformer(spark)
    transformed_df = transformer.run(raw_df)
    transformed_df.write.mode("overwrite").partitionBy("transaction_year", "transaction_month").parquet("s3://dataflow360-processed/customer_sales/")
    spark.stop()
