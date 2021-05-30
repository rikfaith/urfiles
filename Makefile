# Makefile

.PHONY: lint

DLIST:=missing-function-docstring,missing-module-docstring
DLIST:=$(DLIST),missing-class-docstring,too-few-public-methods
DLIST:=$(DLIST),too-many-arguments,too-many-locals,too-many-instance-attributes
DLIST:=$(DLIST),too-many-branches,too-many-statements

PROJECT=urfiles

lint:
	pep8 --ignore=E402 bin/$(PROJECT)
	pep8 $(PROJECT)/*.py
	pylint --disable=$(DLIST) \
		--include-naming-hint=y \
		--good-names=fp \
		$(PROJECT)

wheel:
	python3 setup.py sdist bdist_wheel

