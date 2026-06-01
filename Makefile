# go-db2i developer convenience targets.
#
# Offline targets need no database. The live conformance targets read the
# connection from the environment (DB2I_DSN, DB2I_SCHEMA, ...); source one
# of the gitignored env files first, e.g.:
#
#     set +x && source .env.pub400-v7r5 && make test-conformance
#
# Never put credentials on a command line -- pass them via the environment.

GO        ?= go
# Honor an explicit `go env GOBIN`, else fall back to $GOPATH/bin.
GOBIN     := $(or $(shell $(GO) env GOBIN),$(shell $(GO) env GOPATH)/bin)
COVERFILE ?= coverage.out
COVERPKG  ?= ./...
# Diff base for the "lint only what changed" gate (mirrors CI).
LINT_BASE ?= origin/main

.DEFAULT_GOAL := help

## help: list available targets
.PHONY: help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -e 's/^## //'

## build: compile all packages
.PHONY: build
build:
	$(GO) build ./...

## vet: go vet (offline + conformance-tag compile-check)
.PHONY: vet
vet:
	$(GO) vet ./...
	$(GO) vet -tags=conformance ./...

## fmt: gofmt the tree in place
.PHONY: fmt
fmt:
	gofmt -w .

## fmt-check: fail if any file needs gofmt
.PHONY: fmt-check
fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi

## test: offline unit tests
.PHONY: test
test:
	$(GO) test ./...

## test-race: offline unit tests under the race detector
.PHONY: test-race
test-race:
	$(GO) test -race ./...

## cover: offline tests with a coverage profile + function summary
.PHONY: cover
cover:
	$(GO) test ./... -coverpkg=$(COVERPKG) -coverprofile=$(COVERFILE) -covermode=atomic
	$(GO) tool cover -func=$(COVERFILE) | tail -1

## test-conformance: live conformance suite (needs DB2I_DSN in env)
.PHONY: test-conformance
test-conformance:
	$(GO) test -tags=conformance -v -timeout 30m ./test/conformance/...

## test-conformance-short: live conformance suite, skipping the slow stress tests
.PHONY: test-conformance-short
test-conformance-short:
	$(GO) test -tags=conformance -short -v -timeout 20m ./test/conformance/...

## cover-conformance: live conformance suite with driver+hostserver coverage
.PHONY: cover-conformance
cover-conformance:
	$(GO) test -tags=conformance -timeout 30m \
		-coverpkg=$(COVERPKG) -coverprofile=conformance-$(COVERFILE) -covermode=atomic \
		./test/conformance/...
	$(GO) tool cover -func=conformance-$(COVERFILE) | tail -1

## lint: run golangci-lint over the whole tree (shows pre-existing findings)
.PHONY: lint
lint:
	$(GOBIN)/golangci-lint run ./...

## lint-new: lint only what changed vs LINT_BASE (mirrors the CI gate)
.PHONY: lint-new
lint-new:
	$(GOBIN)/golangci-lint run --new-from-merge-base=$(LINT_BASE) ./...

## ci-offline: everything the always-on CI job runs, offline
.PHONY: ci-offline
ci-offline: build fmt-check vet test cover
