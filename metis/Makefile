.PHONY: clean run
run:
	python runserver.py --debug --reload
clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*.*~' -delete
test: clean
	python runtests.py
installdeps:
	cat packages.txt | xargs sudo apt-get -y install
	sudo pip install -r requirements.txt