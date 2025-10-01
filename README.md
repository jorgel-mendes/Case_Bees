# Breweries Data Pipeline

This project implements a data pipeline to fetch, transform, and aggregate breweries data from the Open Brewery DB API, following the medallion architecture.

## Architecture

- **Bronze Layer**: Raw data fetched from API, stored as JSON.
- **Silver Layer**: Cleaned data, deduplicated, nulls handled, stored as Parquet partitioned by country.
- **Gold Layer**: Aggregated counts of breweries by type and country, stored as Parquet. I also created an excel file as an example of deliverable for a final user.

## Technologies

- Python
- Airflow for orchestration
- Requests for API
- Pandas for transformation
- PyArrow for Parquet
- Docker for containerization

## Design Choices

- Used Airflow for orchestration with DAG and PythonOperators.
- Retry up to 3 times in case of failure in the api request.
- Partitioned silver by country for location-based queries.
- Kept transformations minimal: dedup by id, fill null countries with 'unknown'.
- No schema enforcement beyond basic cleaning, as data is simple.
    - If it were more complicated or critical to other process I would enforce a schema or raise error on changes.

## Trade-offs

- Local storage instead of cloud for simplicity.
- No advanced error handling or retries beyond Airflow defaults.
- Fetched all data at once; in production, consider incremental loads.

## How to Run

### With Docker (Airflow)

```bash
docker build -t bees-case .
docker run -p 8080:8080 bees-case
```

Then, open http://localhost:8080, login with admin/admin, and trigger the 'breweries_pipeline' DAG.

### With Docker (Python)

To run the pipeline directly with Python without Airflow:

```bash
docker build -t bees-case .
docker run --rm bees-case python run.py
```

This will execute the pipeline sequentially: fetch, transform, aggregate.

For docker use -v your_directory:/app/data to get the data in your current folder.

### Locally

1. Install dependencies: `pip install -r docker/requirements.txt`
2. Run Airflow: `airflow standalone`
3. Access http://localhost:8080, trigger the DAG.

## Tests

Run tests: `pytest`

## Monitoring and Alerting

- **Pipeline Failures**: Airflow UI shows task statuses; configure email/Slack alerts on failure.
- **Data Quality**: Add checks for row counts, null percentages; log warnings in task logs.
- **Logs**: Airflow logs tasks; integrate with ELK for advanced monitoring.
- In production, use Airflow's alerting features.