.PHONY: \
	dev \
	clean \
	unit \
	integration \
	tests \
	linter_src \
	linter_tests \
	linter \
	linter_check_src \
	linter_check_tests \
	linter_check \
	types_src \
	types_tests \
	types \
	format_src \
	format_tests \
	format \
	format_check_src \
	format_check_tests \
	format_check \


all: format_check linter_check types tests


install:
	pip install .


install-dev:
	pip install '.[dev]'


install-edev:
	pip install -Ue '.[dev]'


clean:
	-rm -f .coverage
	-rm -f coverage.xml
	-rm -rf `find . -type d -name htmlcov`
	-rm -rf `find . -type d -name *.log`
	-rm -rf `find . -type d -name .pytest_cache`
	-rm -rf `find . -type d -name .ruff_cache`
	-rm -rf `find . -type d -name __pycache__`
	-rm -rf `find . -type d -name .mypy_cache`
	-rm -rf `find . -type d -name '*.egg-info'`
	-rm -f `find . -type f -name '*.py[co]'`
	-rm -rf build dist


unit:
	pytest tests/unit


integration:
	pytest tests/integration


tests: unit integration


linter_src:
	ruff ./src/


linter_tests:
	ruff ./tests/


linter: linter_src linter_tests


linter_check_src:
	ruff check ./src/


linter_check_tests:
	ruff check ./tests/


linter_check: linter_check_src linter_check_tests


types_src:
	pyright ./src/


types_tests:
	pyright ./tests/


types: types_src types_tests


format_src:
	ruff format ./src/


format_tests:
	ruff format ./tests/


format: format_src format_tests


format_check_src:
	ruff format --check ./src/


format_check_tests:
	ruff format --check ./tests/


format_check: format_check_src format_check_tests
