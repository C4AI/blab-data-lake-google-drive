SHELL=bash
PYTHON_VERSION := $(shell sed -r 's/^python_version *= *"?(3\.10)"?/\1/p;d' Pipfile)


dev:
	@if ! command -v python${PYTHON_VERSION} &> /dev/null; \
	then \
		echo "ERROR: Python ${PYTHON_VERSION} is not installed."; \
		exit 1 ; \
	fi
	@echo "Starting development environment..."
	pipenv install --dev
	pipenv shell


doc:
	sphinx-apidoc --separate --module-first  -o docs blabgddatalake/ 
	make -C docs html
