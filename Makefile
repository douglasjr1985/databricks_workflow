# Define variables
PYTHON := python3
PIP := pip

# Install dependencies
install:
	$(PIP) install -r requirements.txt

test:
	$(PYTHON) -m unittest discover -s tests

# List modified files
list-modified-files:
	if [ "${{ github.event.head_commit.id }}" != "${{ github.event.before }}" ]; then \
		git diff --name-status ${{ github.event.before }} ${{ github.event.head_commit.id }} | awk '{print $2}' | sed -e 's/.*\///' -e 's/\..*$$//' > changed-files.txt; \
	else \
		echo "No changes in this push."; \
	fi

# Deploy your application
deploy:
	# Add deployment commands here
	cat changed-files.txt | while read -r filename; do \
		$(PYTHON) toolkit/main.py --filename "$$filename"; \
	done

# Default target
all: install test list-modified-files deploy
