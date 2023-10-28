# Define variables
PYTHON := python3
PIP := pip
PYTEST := pytest

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# Run tests using pytest
test:
	$(PYTEST) tests

# List modified files
list-modified-files:
	if [ "$(GITHUB_HEAD_COMMIT_ID)" != "$(GITHUB_BEFORE_COMMIT)" ]; then \
		git diff --name-status $(GITHUB_BEFORE_COMMIT) $(GITHUB_HEAD_COMMIT_ID) | awk -F/ '{print $$2}' | grep '\.json$$' | sed 's/\.json$$//' > changed-files.txt; \
	else \
		echo "No changes in this push."; \
	fi

# Run the Python script with modified files and deploy
deploy:
	while IFS= read -r filename; do \
		$(PYTHON) toolkit/main.py --filename "$$filename"; \
	done < changed-files.txt

# Default target
all: install test list-modified-files deploy