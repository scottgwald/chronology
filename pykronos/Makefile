.PHONY: clean
clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*~' -delete
clean_egg: clean
	sudo rm -rf pykronos.egg-info
	sudo rm -rf build
	sudo rm -rf dist
test: clean
	python runtests.py
readme: clean
	python ../common/readmeify.py < demo.py > README.md
