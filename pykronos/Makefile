.PHONY: clean
clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*~' -delete
	sudo rm -rf build dist *.egg-info
test: clean
	python runtests.py
readme: clean
	python ../common/readmeify.py < demo.py > README.md
