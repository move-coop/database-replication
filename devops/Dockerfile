FROM python:3.12-bullseye

WORKDIR /app

# Cache dependencies to speed up builds
COPY devops/requirements.txt .
RUN pip install -r requirements.txt

COPY ./src /app/src
WORKDIR /app/src

CMD ["python", "import_single_table_to_bigquery.py"]