import os
import logging
from pyspark.sql.functions import col, trim

logging.basicConfig(level=logging.INFO)

def transform_to_silver(spark, input_path, output_path):
    logging.info("Starting silver transformation")
    df = spark.read.option("multiline", "true").json(input_path)

    logging.info(f"Loaded {df.count()} raw records")
    # Clean data
    df = df.dropDuplicates(['id'])
    df = df.fillna({'state': 'unknown', 'country': 'unknown'})
    df = df.withColumn('country', trim(col('country')))

    logging.info(f"After cleaning: {df.count()} records")
    # Save partitioned by country
    df.write.mode("overwrite").partitionBy("country").parquet(output_path)
    logging.info(f"Saved silver data partitioned by country to {output_path}")
