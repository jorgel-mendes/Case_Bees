from src.fetch import fetch_breweries, save_raw_data
from src.transform import transform_to_silver
from src.aggregate import aggregate_to_gold

if __name__ == "__main__":
    print("Starting pipeline...")
    data = fetch_breweries()
    save_raw_data(data, 'data/bronze/breweries_raw.json')
    print("Bronze done.")
    transform_to_silver('data/bronze/breweries_raw.json', 'data/silver/breweries_silver.parquet')
    print("Silver done.")
    aggregate_to_gold('data/silver/breweries_silver.parquet', 'data/gold/breweries_agg.parquet')
    print("Gold done. Pipeline complete.")