.PHONY: clean test
clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*.*~' -delete
test: clean
	python run_tests.py all
installdeps:
	cat packages.txt | xargs sudo apt-get -y install
	sudo pip install -r requirements.txt