run:
	@docker compose up --build;
	

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