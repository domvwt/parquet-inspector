.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

.PHONY: help
help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

.PHONY: clean
clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

.PHONY: clean-build
clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

.PHONY: clean-pyc
clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

.PHONY: clean-test
clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

.PHONY: format
format: ## apply black code formatter
	black .

.PHONE: lint
lint: ## check style with flake8
	flake8 src tests

.PHONY: mypy
mypy: ## check type hints
	mypy src

.PHONY: isort
isort: ## sort imports
	isort src tests

.PHONY: cqa
cqa: format isort lint mypy ## run all cqa tools

.PHONY: test
test: ## run tests quickly with the default Python
	pytest

.PHONY: test-all
test-all: ## run tests on every Python version with tox
	tox --skip-missing-interpreters
	python -m tests.check_package_version

.PHONY: coverage
coverage: ## check code coverage quickly with the default Python
	-coverage run --source src -m pytest
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

.PHONY: reqs
reqs: ## output requirements.txt
	poetry export -f requirements.txt -o requirements.txt --without-hashes

.PHONY: release
release: dist ## package and upload a release
	twine upload dist/*

.PHONY: dist
dist: clean ## builds source and wheel package
	poetry build
	ls -l dist

.PHONY: hooks
hooks: ## run pre-commit hooks on all files
	pre-commit run -a

.PHONY: install
install: clean ## install the package to the active Python's site-packages
	poetry install
