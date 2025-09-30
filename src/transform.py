import pandas as pd
import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import dataset

logging.basicConfig(level=logging.INFO)

def transform_to_silver(input_path, output_path):
    logging.info("Starting silver transformation")
    with open(input_path, 'r') as f:
        data = pd.read_json(f)

    logging.info(f"Loaded {len(data)} raw records")
    # Clean data
    data = data.drop_duplicates(subset=['id'])
    data['state'] = data['state'].fillna('unknown')
    data['country'] = data['country'].fillna('unknown').str.strip()

    logging.info(f"After cleaning: {len(data)} records")
    # Save as partitioned Parquet files by country
    data.to_parquet(output_path, partition_cols=['country'], index=False)
    logging.info(f"Saved silver data partitioned by country to {output_path}")