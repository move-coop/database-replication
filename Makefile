run:
	@if [ ! -z $(build) ]; then \
		docker compose up --build; \
	else \
		docker compose up; \
	fi


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