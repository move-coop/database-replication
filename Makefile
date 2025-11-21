dlt:
	@docker compose up tmc-dlt --build;
	

ruff:
	@ruff check --fix .
	@ruff format .


docker:
	@docker build \
		-t ianrichardferguson/tmc-dlt:latest \
		-f ./devops/Dockerfile \
		--platform="linux/amd64" \
		.

	@docker push ianrichardferguson/tmc-dlt:latest

trino:
	@docker compose up tmc-trino --build;

trino-shell:
	@docker compose up tmc-trino --build -d;
	@docker compose exec -it tmc-trino /bin/bash;


# NOTE - This is just a QOL target to help with establishing an SSH tunnel to the Dataproc cluster.
# E.g., make tunnel PORT=8060
tunnel:
	@if [ -z "$$PORT" ]; then \
		echo "Error: PORT environment variable is not set."; \
		exit 1; \
	fi
	@echo "Establishing SSH tunnel on port $$PORT..."
	@gcloud compute ssh \
    --zone "us-central1-c" \
    "data-transfer-cluster-m" \
    --tunnel-through-iap \
    --project "tmc-data-transfer" \
    -- \
    -L $$PORT:localhost:$$PORT