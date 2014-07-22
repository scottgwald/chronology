.PHONY: clean run
run:
	python runserver.py --debug --reload
clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*.*~' -delete
	sudo rm -rf build dist *.egg-info logs
test: clean
	python runtests.py all
benchmark: clean
	python runbenchmarks.py all
installdeps:
	cat packages.txt | xargs sudo apt-get -y install
	sudo pip install -r requirements.txt
install: installdeps clean
	sudo python setup.py install
