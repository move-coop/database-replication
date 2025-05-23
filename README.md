# TMC DLT Database Replication

This is a demo-y version of what a potential `dlt-to-bigquery` operator might look like at TMC. It requires minimal input, and ought to be flexible enough to write from any JDBC connection back to our data warehouse.

## Setup
For local development you'll want to populate a `test.env` file for whatever connection data is relevant. I recommend setting `DESTINATION_USE_JSON=true` and adding the relative path from `src` to the service account of your choice

```
export DEBUG=true
export GOOGLE_APPLICATION_CREDENTIALS="../service_accounts/dev.json"

export SOURCE_DRIVERNAME="< FILL THIS IN >"
export SOURCE_HOST="34.86.233.< FILL THIS IN >"
export SOURCE_DATABASE="< FILL THIS IN >"
export SOURCE_PORT="< FILL THIS IN >"
export SOURCE_USERNAME="< FILL THIS IN >"
export SOURCE_PASSWORD="< FILL THIS IN >"

export DESTINATION_LOCATION="< FILL THIS IN >"
export DESTINATION_USE_JSON="< FILL THIS IN >"

export VENDOR_NAME="< FILL THIS IN >"
export SOURCE_SCHEMA_NAME="< FILL THIS IN >"
export SOURCE_TABLE_NAME="< FILL THIS IN >"
export DESTINATION_SCHEMA_NAME="< FILL THIS IN >"
export DESTINATION_TABLE_NAME="< FILL THIS IN >"
export FULL_REFRESH="< FILL THIS IN >"
```

Once you have this environment information sorted, just run `make run build=true` (once you do this the first time you won't have to include the `build` argument)