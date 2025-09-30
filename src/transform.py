import os
import logging

logging.basicConfig(level=logging.INFO)

def transform_to_silver(spark, input_path, output_path):
    logging.info("Starting silver transformation")
    df = spark.read.option("multiline", "true").json(input_path)

    logging.info(f"Loaded {df.count()} raw records")
    # Clean data
    df = df.dropDuplicates(['id'])
    df = df.fillna({'state': 'unknown', 'country': 'unknown'})

    logging.info(f"After cleaning: {df.count()} records")
    # Save partitioned by state
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").partitionBy("state").parquet(output_path)
    logging.info(f"Saved silver data to {output_path}")