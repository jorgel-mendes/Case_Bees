FROM python:3.11-slim

RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV AIRFLOW__CORE__DAGS_FOLDER=/app/src
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV PYTHONPATH=/app

RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]
