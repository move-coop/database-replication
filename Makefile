run:
	@if [ ! -z $(build) ]; then \
		docker compose up --build; \
	else \
		docker compose up; \
	fi
