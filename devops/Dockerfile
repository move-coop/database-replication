FROM python:3.12-bullseye

# Install jq binary
RUN apt-get update && apt-get install -y jq && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies to speed up builds
COPY devops/requirements.txt .
RUN pip install -r requirements.txt

COPY ./src /app/src
WORKDIR /app/src

CMD ["python", "import_single_table_to_bigquery.py"]