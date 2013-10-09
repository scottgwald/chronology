.PHONY: clean test
clean:
	find . -type f -name '*.py[cod~]' -delete
test: clean
	python run_tests.py