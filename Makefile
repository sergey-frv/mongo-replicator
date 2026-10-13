REVISION := $(shell git describe --always --dirty 2>/dev/null || echo unknown)

.PHONY: default
default: docker-volumes run ## Init docker volumes, start mongo and replicator

.PHONY: build
build:
	go mod tidy
	go mod vendor
	go build -ldflags "-X main.revision=$(REVISION)" -o bin/mongo-replicator ./cmd/mongo-replicator

.PHONY: lint
lint:
	docker compose run --rm --no-deps linter /bin/sh -c "\
		CGO_ENABLED=1 golangci-lint run ./... -c .golangci.yml -v"

.PHONY: test
test:
	docker compose run --rm --no-deps app /bin/sh -c "\
		CGO_ENABLED=1 go test -race -timeout=\"300s\" ./..."

# -count=1: иначе go-build-cache отдаёт (cached) без обращения к MongoDB
.PHONY: test-integration
test-integration:
	docker compose run --rm app /bin/sh -c "\
		CGO_ENABLED=1 go test -count=1 -race -p 1 -timeout=\"600s\" -tags integration ./..."

# без --start-ts на пустом чекпойнте Run вернёт ErrNoStartPoint
.PHONY: run
run:
	docker compose run --rm app /bin/sh -c "\
		go run -ldflags \"-X main.revision=$(REVISION)\" ./cmd/mongo-replicator --dbg --start-ts \$$(date +%s)"

# у mongo-образов VOLUME /data/db — без -v остаются анонимные тома
.PHONY: down
down:
	docker compose down -v

.PHONY: docker-volumes
docker-volumes: ## Create docker cache volumes
	docker volume create go-build-cache
	docker volume create go-lint-cache
