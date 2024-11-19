.DEFAULT_GOAL := help


help:
	@echo "make help					- Prints this command and exit"
	@echo "			------ Working with docker compose -----"
	@echo "make linters					- Run all linters that were configured for project"

# ---------- Linters ----------
linters:
	@echo "-------- running black --------"
	@poetry run black .
	@echo "-------- running flake8 --------"
	@poetry run flake8 --config pyproject.toml .

black:
	@poetry run black .
flake8:
	@poetry run flake8 --config pyproject.toml .
