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
	if [ "${{ github.event.head_commit.id }}" != "${{ github.event.before }}" ]; then \
		git diff --name-status ${{ github.event.before }} ${{ github.event.head_commit.id }} | awk '{print $2}' | sed -e 's/.*\///' -e 's/\..*$$//' > changed-files.txt; \
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