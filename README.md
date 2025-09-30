# Breweries Data Pipeline

This project implements a data pipeline to fetch, transform, and aggregate breweries data from the Open Brewery DB API, following the medallion architecture.

## Architecture

- **Bronze Layer**: Raw data fetched from API, stored as JSON.
- **Silver Layer**: Cleaned data, deduplicated, nulls handled, stored as Parquet partitioned by state.
- **Gold Layer**: Aggregated counts of breweries by type and state, stored as Parquet.

## Technologies

- Python
- PySpark for data processing
- Requests for API fetching
- Docker for containerization

## Design Choices

- Used PySpark for scalable data processing, replacing Pandas for larger datasets.
- Partitioned silver by state for efficient location-based queries.
- Kept transformations minimal: dedup by id, fill null states with 'unknown'.
- Simple sequential execution in run.py for ease of understanding.

## Trade-offs

- Local storage instead of cloud for simplicity.
- No advanced orchestration; simple script run.
- Fetched all data at once; in production, consider incremental loads.

## How to Run

### With Docker

```bash
docker build -t bees-case .
docker run bees-case
```

The pipeline runs automatically and completes.

### Locally

1. Install dependencies: `pip install -r docker/requirements.txt`
2. Ensure Java is installed for PySpark.
3. Run: `python run.py`

## Tests

Run tests: `pytest`

## Monitoring and Alerting

- **Pipeline Failures**: Check logs in terminal; in production, wrap in monitoring tool.
- **Data Quality**: Add checks for row counts, null percentages; log in code.
- **Logs**: Python logging; integrate with logging services.
- For production, use tools like Apache Airflow or Prefect for orchestration and monitoring.