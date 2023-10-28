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
				"${github.event.head_commit.id}" 

# Run the Python script with modified files and deploy # echo >> changed-files.txt
deploy:
	
	cat changed-files.txt | while read -r filename; do \
		python3 toolkit/main.py --filename "$$filename"; \
	done
	
# Default target
all: install test list-modified-files deploy