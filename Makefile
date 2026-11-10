.PHONY: generate lint test check

# Pinned: the same version CI runs.
GOLANGCI_LINT_VERSION ?= v2.15.0
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

# Regenerate Go code from the protos.
generate:
	cd api && buf generate

lint:
	cd api && buf lint
	$(GOLANGCI_LINT) run ./...

test:
	go test -race ./...

# What CI runs.
check: lint test
