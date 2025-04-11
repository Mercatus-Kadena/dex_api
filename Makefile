VERSION=0.12.3

#DOCKER_IMAGE=dex_api
DOCKER_IMAGE=ghcr.io/mercatus-kadena/dex_api

.PHONY: all docker wheel clean

all: docker wheel

dist/.docker-%:
	mkdir -p dist
	docker build . --build-arg CONFIG=$(word 2,$(subst -, ,$*)) -t ${DOCKER_IMAGE}:$*
	touch $@

dist/%.whl:
	mkdir -p dist
	python3 -m build


docker: dist/.docker-${VERSION}-tcp dist/.docker-${VERSION}-unix

wheel: dist/dex_api-${VERSION}-py3-none-any.whl

clean:
	rm -rf dist
