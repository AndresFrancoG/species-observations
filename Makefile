# Variables
VENV_NAME := .venv
PYTHON_BIN := python3
PIP_BIN := $(VENV_NAME)/bin/pip
DOCKER_REPO := andresfrancog/species-observations

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
	@echo "  docker_push   Pushes docker image with version 'latest' to remote repository.  Modify the variable DOCKER_REPO in makefile as needed."

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
	@$(PIP_BIN) install -r src/requirements.txt

lint:
	.venv/bin/black .

test:
	.venv/bin/jupyter nbconvert --clear-output --inplace $(SOURCES)
	.venv/bin/pytest --nbval $(SOURCES)

test_cloud:
	.venv/bin/jupyter nbconvert --clear-output --inplace notebooks/driver.ipynb $(SOURCES_CLOUD)
	.venv/bin/pytest --nbval notebooks/driver.ipynb $(SOURCES_CLOUD)

clean:
	rm -rf $(VENV_NAME)

all: create_venv install test clean

docker_push:
	docker tag species-observations:latest $(DOCKER_REPO):latest
	docker push  $(DOCKER_REPO):latest