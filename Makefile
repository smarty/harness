#!/usr/bin/make -f

test: fmt
	go test -timeout=1s -short -race -covermode=atomic ./...

test.pool: test
	go test -timeout=1s -run TestPoolHygieneFixture github.com/smarty/harness/v2/internal/pipeline

test.db: test.pool
	go test -timeout=30s -race -covermode=atomic github.com/smarty/harness/v2/sqladapter

test.db.local:
	(docker compose -f doc/docker-compose.yml up --wait && $(MAKE) test.db --no-print-directory); docker compose -f doc/docker-compose.yml down

fmt:
	go mod tidy && go fmt ./...

compile:
	go build ./...

build: test compile

.PHONY: test fmt compile build
