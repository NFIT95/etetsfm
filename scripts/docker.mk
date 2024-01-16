MODULE = $(shell basename '$(CURDIR)')

.PHONY: build

build:
	docker build -f Dockerfile -t $(MODULE):latest ../..
