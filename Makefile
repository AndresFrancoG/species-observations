# Variables
VENV_NAME := .venv
PYTHON_BIN := python3
PIP_BIN := $(VENV_NAME)/bin/pip

# Set the root directory where subdirectories and notebooks are located
ROOT_DIR := notebooks

# Find all subdirectories recursively
SOURCES := $(wildcard $(ROOT_DIR)/*.ipynb) $(wildcard $(ROOT_DIR)/**/*.ipynb)

.PHONY: help create_venv install clean test

help:
	@echo "Available targets:"
	@echo "  create_venv   Create a virtual environment"
	@echo "  install       Installs dependencies from requirements.txt"
	@echo "  test          Runs unit tests for the project and checks if notebooks execute correctly"
	@echo "  clean         Remove the virtual environment and installed packages"
	@echo "  all           Creates virtual env, intalls dependencies, performs tests, and cleans environment"

create_venv:
	$(PYTHON_BIN) -m venv $(VENV_NAME)
	python3 -m ipykernel install --user --name=<projectname>

install_gcp:
	curl -o /tmp/google-cloud-cli-431.0.0-linux-x86_64.tar.gz https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-431.0.0-linux-x86_64.tar.gz
	mkdir -p /tmp/gcp_cli && tar -xf /tmp/google-cloud-cli-431.0.0-linux-x86_64.tar.gz -C /tmp/gcp_cli
	chmod +x /tmp/gcp_cli/google-cloud-sdk/install.sh
	bash /tmp/gcp_cli/google-cloud-sdk/install.sh
	/tmp/gcp_cli/google-cloud-sdk/bin/gcloud init
	gcloud auth application-default login
	
install:
	@$(PIP_BIN) install --upgrade pip
	@$(PIP_BIN) install -r requirements.txt

test:
	pytest --nbval $(SOURCES)

clean:
	rm -rf $(VENV_NAME)

all: create_venv install test clean
