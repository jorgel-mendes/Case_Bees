FROM python:3.11-slim

WORKDIR /app

COPY docker/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV AIRFLOW__CORE__DAGS_FOLDER=/app/src
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

CMD ["airflow", "standalone"]