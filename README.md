# TMC DLT Database Replication

This is a demo-y version of what a potential `dlt-to-bigquery` operator might look like at TMC. It requires minimal input, and ought to be flexible enough to write from any JDBC connection back to our data warehouse.

## Setup
For local development you'll want to populate a `test.env` file for whatever connection data is relevant. I recommend setting `DESTINATION_USE_JSON=true` and adding the relative path from `src` to the service account of your choice

See the [example .env file](test.env.example) for all the required environment variables.

Once you have this environment information sorted, just run `make run build=true` (once you do this the first time you won't have to include the `build` argument)