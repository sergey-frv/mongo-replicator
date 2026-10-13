REVISION := $(shell git describe --always --dirty 2>/dev/null || echo unknown)

.PHONY: build
build:
	go mod tidy
	go mod vendor
	go build -ldflags "-X main.revision=$(REVISION)" -o bin/mongo-replicator ./cmd/mongo-replicator
