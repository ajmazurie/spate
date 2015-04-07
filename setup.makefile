
PROJECT_NAME := spate

SHELL := /bin/bash
BOLD := \033[1m
DIM := \033[2m
RESET := \033[0m

all: uninstall install clean test

uninstall:
	@echo -e "$(BOLD)uninstalling '$(PROJECT_NAME)'$(RESET)"
	@pip uninstall -y $(PROJECT_NAME)

install:
	@echo -e "$(BOLD)installing '$(PROJECT_NAME)'$(RESET)"
	@echo -e -n "$(DIM)"
	@./setup.py install
	@echo -e -n "$(RESET)"

dist:
	@echo -e "$(BOLD)packaging '$(PROJECT_NAME)'$(RESET)"
	@./setup.py sdist --formats=zip

clean:
	@echo -e "$(BOLD)cleaning '$(PROJECT_NAME)' repository$(RESET)"
	@rm -rf build
	@rm -rf dist
	@rm -rf **/$(PROJECT_NAME).egg-info
	@rm -rf $(PROJECT_NAME).egg-info

test:
	@echo -e "$(BOLD)running test units for '$(PROJECT_NAME)'$(RESET)"
	@python -m unittest discover \
		-s tests -p 'tests_*.py' \
		--verbose
	@rm -rf tests/*.pyc
