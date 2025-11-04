# Trino Connector

This approach is more abstract compared to the `dlt` implementation. We pass a `SourceClient` and a `TrinoClient` object into a `ClientConnector`, set the runtime values (row chunk size, write disposition, etc.) and run the `.transfer()` function.

## Local Development

From Cody, we currently need to port forward through an SSH tunnel like so:
```bash
gcloud compute ssh \
    --zone "us-central1-c" \
    "data-transfer-cluster-m" \
    --tunnel-through-iap \
    --project "tmc-data-transfer" \
    -- \
    -L 8060:localhost:8060
```

You can open the browser and go to `localhost:8060` - if the port forwarding is working you'll see the Trino UI displayed with recent job submissions.
