import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)

def transform_to_silver(input_path, output_path):
    logging.info("Starting silver transformation")
    with open(input_path, 'r') as f:
        data = pd.read_json(f)

    logging.info(f"Loaded {len(data)} raw records")
    # Clean data
    data = data.drop_duplicates(subset=['id'])
    data['state'] = data['state'].fillna('unknown')
    data['country'] = data['country'].fillna('unknown')

    logging.info(f"After cleaning: {len(data)} records")
    # Save as single Parquet file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    data.to_parquet(output_path, index=False)
    logging.info(f"Saved silver data to {output_path}")