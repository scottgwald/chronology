.PHONY: clean
clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*~' -delete
	find . -type f -name '*.py,cover' -delete
	sudo rm -rf build dist *.egg-info
test: clean
	python runtests.py
install:
	python setup.py install
testcoverage: clean
	coverage run runtests.py
htmlcoverage: clean
	coverage html -d coverage_html
readme: clean
	python pykronos/common/readmeify.py < demo.py > README.md
