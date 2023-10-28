# Define variables
PYTHON := python3
PIP := pip

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# List modified files
list-modified-files:
	if [ "${{ github.event.head_commit.id }}" != "${{ github.event.before }}" ]; then \
		git diff --name-status ${{ github.event.before }} ${{ github.event.head_commit.id }} | awk '{print $2}' | sed -e 's/.*\///' -e 's/\..*$$//' > changed-files.txt; \
	else \
		echo "No changes in this push."; \
	fi

# Run the Python script with modified files
run-python-script:
	cat changed-files.txt | while read -r filename; do \
		$(PYTHON) toolkit/main.py --filename "$$filename"; \
	done

# Deploy your application
deploy:
	# Add deployment commands here

# Default target
all: install list-modified-files run-python-script deploy
