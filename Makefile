# Makefile

.PHONY: all build-dashboard build-consumer build-producer

all: build-dashboard build-consumer build-producer


build-dashboard:
	docker build -t dashboard -f dashboard/Dockerfile dashboard/.

build-producer:
	docker build -t producer -f producer/Dockerfile producer/.

build-consumer:
	docker build -t consumer -f consumer/Dockerfile consumer/.
