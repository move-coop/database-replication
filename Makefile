dlt:
	@docker compose up tmc-dlt --build;
	
sling:
	@docker compose up tmc-sling --build -d;
	
sling-shell:
	@docker compose exec -it tmc-sling bash

ruff:
	@ruff check --fix .
	@ruff format .


docker_dlt:
	@docker build \
		-t ianrichardferguson/tmc-dlt:latest \
		-f ./devops/Dockerfile \
		--platform="linux/amd64" \
		.

	@docker push ianrichardferguson/tmc-dlt:latest

docker_sling:
	@docker build \
		-t wraedy/tmc-sling:latest \
		-f ./devops/Dockerfile.sling \
		--platform="linux/amd64" \
		.

	@docker push wraedy/tmc-slingt:latest