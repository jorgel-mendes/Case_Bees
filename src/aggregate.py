import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)

def aggregate_to_gold(input_path, output_path):
    logging.info("Starting gold aggregation")
    data = pd.read_parquet(input_path)

    logging.info(f"Loaded {len(data)} records from silver")
    # Aggregate
    agg = data.groupby(['brewery_type', 'country'], observed=False).size().reset_index(name='count')

    logging.info(f"Aggregated to {len(agg)} groups")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    agg.to_parquet(output_path, index=False)
    excel_path = output_path.replace('.parquet', '.xlsx')
    agg.to_excel(excel_path, index=False)
    logging.info(f"Saved gold data to {output_path} and {excel_path}")