SHELL=bash

dev:
	@echo "Starting development environment..."
	poetry install --with dev,docs,test
	poetry shell


doc:
	sphinx-apidoc -f --separate --module-first  -o "docs" "blabgddatalake/"
	make -C "docs" html

showdoc: doc
	xdg-open "docs/_build/html/index.html"
