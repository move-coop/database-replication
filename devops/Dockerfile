FROM python:3.12-bullseye

WORKDIR /app

# Cache dependencies to speed up builds
COPY devops/requirements.txt .
RUN pip install -r requirements.txt

COPY ./src /app/src
WORKDIR /app/src

# NOTE - This is to offset dlt's native behavior or normalizing destination schemas
# We never want this enabled, so it's toggled off here by default
ENV DESTINATION__BIGQUERY__ENABLE_DATASET_NAME_NORMALIZATION=false

CMD ["python", "import_single_table_to_bigquery.py"]