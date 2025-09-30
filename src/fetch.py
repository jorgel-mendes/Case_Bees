import requests
import json
import os
import logging

logging.basicConfig(level=logging.INFO)

def fetch_breweries():
    base_url = "https://api.openbrewerydb.org/v1/breweries"
    all_breweries = []
    page = 1
    per_page = 50

    while True:
        logging.info(f"Fetching page {page}")
        response = requests.get(base_url, params={"page": page, "per_page": per_page})
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        all_breweries.extend(data)
        page += 1

    logging.info(f"Total breweries fetched: {len(all_breweries)}")
    return all_breweries

def save_raw_data(data, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    logging.info(f"Saved raw data to {output_path}")