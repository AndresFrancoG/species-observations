# Variables
VENV_NAME := .venv
PYTHON_BIN := python3
PIP_BIN := $(VENV_NAME)/bin/pip

# Set the root directory where subdirectories and notebooks are located
ROOT_DIR := notebooks

# Find all subdirectories recursively
SOURCES := $(wildcard $(ROOT_DIR)/*.ipynb) $(wildcard $(ROOT_DIR)/**/*.ipynb)

# Find all subdirectories recursively, only keeps notebooks with name starting with ct_
SOURCES_CLOUD := $(wildcard $(ROOT_DIR)/ct_*.ipynb) $(wildcard $(ROOT_DIR)/**/ct_*.ipynb)

.PHONY: help create_venv install clean test

help:
	@echo "Available targets:"
	@echo "  create_venv   Create a virtual environment"
	@echo "  install_gcp   Intall and initializes gcp cli"
	@echo "  install       Installs dependencies from requirements.txt"
	@echo "  test          Cleans noteboouk outputs, runs unit tests for the project and checks if notebooks execute correctly using the local env"
	@echo "  test_cloud    Cleans noteboouk outputs, runs unit tests for the project and checks if notebooks execute correctly using the test_cloud env"
	@echo "  clean         Remove the virtual environment and installed packages"
	@echo "  all           Executes: create_venv install test clean"
	@echo "  all_cloud     Executes: create_venv install test_cloud clean"

create_venv:
	$(PYTHON_BIN) -m venv $(VENV_NAME)

install_gcp:
	curl -o /tmp/google-cloud-cli-431.0.0-linux-x86_64.tar.gz https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-431.0.0-linux-x86_64.tar.gz
	mkdir -p /tmp/gcp_cli && tar -xf /tmp/google-cloud-cli-431.0.0-linux-x86_64.tar.gz -C /tmp/gcp_cli
	chmod +x /tmp/gcp_cli/google-cloud-sdk/install.sh
	bash /tmp/gcp_cli/google-cloud-sdk/install.sh
	/tmp/gcp_cli/google-cloud-sdk/bin/gcloud init
	
install:
	@$(PIP_BIN) install --upgrade pip
	@$(PIP_BIN) install -r requirements.txt

test:
	jupyter-nbconvert --clear-output --inplace $(SOURCES)
	pytest --nbval $(SOURCES)

test_cloud:
	jupyter-nbconvert --clear-output --inplace notebooks/driver.ipynb $(SOURCES_CLOUD)
	pytest --nbval notebooks/driver.ipynb $(SOURCES_CLOUD)

clean:
	rm -rf $(VENV_NAME)

all: create_venv install test clean
