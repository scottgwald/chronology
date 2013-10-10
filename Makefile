.PHONY: clean test
clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*.*~' -delete
test: clean
	python run_tests.py all