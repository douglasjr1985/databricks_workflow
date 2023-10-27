.PHONY: install test deploy find_changed_files

install:
    pip install -r requirements.txt

test:
    python -m unittest discover -s tests

deploy:
    python deploy.py

find_changed_files:
    git diff --name-only $(GITHUB_BEFORE) $(GITHUB_SHA) | grep "^resources/"