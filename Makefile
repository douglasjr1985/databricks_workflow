# Define variables
PYTHON := python3
PIP := pip
PYTEST := pytest

# Install dependencies
install:
	$(PIP) install -r requirements.txt

test:
	$(PYTEST) tests

# List modified files
list-modified-files:
			if [ "$(GITHUB_HEAD_COMMIT_ID)" != "$(GITHUB_BEFORE_COMMIT)" ]; then \
				git diff --name-status $(GITHUB_BEFORE_COMMIT) $(GITHUB_HEAD_COMMIT_ID) | awk -F/ '{print $$NF}' | grep '\.py$$' | sed 's/\.py$$//' > changed-files.txt \
			else \
				echo "No changes in this push."; \
			fi    

# Run the Python script with modified files and deploy # echo >> changed-files.txt
deploy:
	
	cat changed-files.txt | while read -r filename; do \
		python3 toolkit/main.py --filename "$$filename"; \
	done
	
# Default target
all: install test list-modified-files deploy
