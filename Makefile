#!/usr/bin/make -f

test: fmt
	go test -timeout=1s -short -covermode=atomic       ./...
	go test -timeout=3s -short -covermode=atomic -race ./...

test.db: test
	go test -tags=integration -timeout=30s -race -covermode=atomic github.com/smarty/harness/v2/storage/mysql

test.db.local:
	(docker compose -f doc/docker-compose.yml up --wait && $(MAKE) test.db --no-print-directory); docker compose -f doc/docker-compose.yml down

fmt:
	go mod tidy && go fmt ./...

compile:
	go build ./...

build: test compile

.PHONY: test test.db test.db.local fmt compile build
