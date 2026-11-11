.PHONY: generate lint test check postgres postgres-stop

# Pinned: the same version CI runs.
GOLANGCI_LINT_VERSION ?= v2.15.0
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

# Regenerate Go code from the protos.
generate:
	cd api && buf generate

lint:
	cd api && buf lint
	$(GOLANGCI_LINT) run ./...

# The PostgreSQL the tests of the PostgreSQL store use. Without CRAWLY_TEST_POSTGRES
# those tests are skipped; `make check` starts a database and runs them.
POSTGRES_CONTAINER ?= crawly-pg-test
POSTGRES_PORT ?= 55432
CRAWLY_TEST_POSTGRES ?= postgres://postgres:crawly@localhost:$(POSTGRES_PORT)/crawly

# Start a throwaway PostgreSQL for the tests, unless one is running already.
postgres:
	@docker inspect -f '{{.State.Running}}' $(POSTGRES_CONTAINER) 2>/dev/null | grep -q true || \
		(docker rm -f $(POSTGRES_CONTAINER) >/dev/null 2>&1; \
		docker run -d --name $(POSTGRES_CONTAINER) -e POSTGRES_PASSWORD=crawly -e POSTGRES_DB=crawly \
			-p $(POSTGRES_PORT):5432 postgres:18 -c fsync=off -c max_connections=200 >/dev/null)
	@until docker exec $(POSTGRES_CONTAINER) pg_isready -q -U postgres -d crawly; do sleep 0.5; done

postgres-stop:
	docker rm -f $(POSTGRES_CONTAINER)

test: postgres
	CRAWLY_TEST_POSTGRES=$(CRAWLY_TEST_POSTGRES) go test -race ./...

# What CI runs.
check: lint test
