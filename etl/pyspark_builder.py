from pyspark.sql import SparkSession
import os
from pyspark.sql.types import (StructField,
                               StructType,
                               StringType,
                               DoubleType)


class SparkDataset:
    def __init__(self):
        java_home = (
            "/Library/Java/JavaVirtualMachines/"
            "temurin-17.jdk/Contents/Home"
        )
        os.environ['JAVA_HOME'] = java_home
        self.spark = SparkSession.builder.appName('CryptoETL').getOrCreate()

    def read(self, path):
        crypto_schema = StructType([
                StructField("id", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("name", StringType(), True),
                StructField("image", StringType(), True),
                StructField("current_price", DoubleType(), True),
                StructField("market_cap", DoubleType(), True),
                StructField("market_cap_rank", DoubleType(), True),
                StructField("fully_diluted_valuation", DoubleType(), True),
                StructField("total_volume", DoubleType(), True),
                StructField("high_24h", DoubleType(), True),
                StructField("low_24h", DoubleType(), True),
                StructField("price_change_24h", DoubleType(), True),
                StructField("price_change_percentage_24h", DoubleType(), True),
                StructField("market_cap_change_24h", DoubleType(), True),
                StructField("market_cap_change_percentage_24h", DoubleType(), True),
                StructField("circulating_supply", DoubleType(), True),
                StructField("total_supply", DoubleType(), True),
                StructField("max_supply", DoubleType(), True),
                StructField("ath", DoubleType(), True),
                StructField("ath_change_percentage", DoubleType(), True),
                StructField("ath_date", StringType(), True),
                StructField("atl", DoubleType(), True),
                StructField("atl_change_percentage", DoubleType(), True),
                StructField("atl_date", StringType(), True),
                StructField("roi_times", DoubleType(), True),
                StructField("roi_currency", StringType(), True),
                StructField("roi_percentage", DoubleType(), True),
                StructField("last_updated", StringType(), True)
                ])

        data = (self.spark.read.schema(crypto_schema)
                .option("multiLine", True)
                .json(path))
        return data

    def save(self, data, path):
        data.coalesce(1).write.mode("overwrite").parquet(path)

    def read_parquet(self, path: str):
        df = self.spark.read.parquet(path)
        df.printSchema()
        return df
