all: run

run: build
	./data_access

build:
	go build -o data_access

get:
	go get ./...

fmt:
	go fmt ./...