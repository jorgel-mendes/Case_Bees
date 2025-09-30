import os
import logging

logging.basicConfig(level=logging.INFO)

def aggregate_to_gold(spark, input_path, output_path):
    logging.info("Starting gold aggregation")
    df = spark.read.parquet(input_path)

    logging.info(f"Loaded {df.count()} records from silver")
    # Aggregate
    agg_df = df.groupBy("brewery_type", "state").count()

    logging.info(f"Aggregated to {agg_df.count()} groups")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    agg_df.write.mode("overwrite").parquet(output_path)
    logging.info(f"Saved gold data to {output_path}")