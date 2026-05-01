.PHONY: build test test-race test-integration vet fmt-check coverage

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

# test-integration brings up an ActiveMQ classic broker via docker compose,
# runs the cimstomp.Client integration tests with the `integration` build
# tag, and tears the broker down. Requires docker compose on PATH.
test-integration:
	docker compose up -d
	# Give ActiveMQ a moment to bind 61613.
	sleep 5
	go test -tags=integration -race ./internal/cimstomp/; \
	  rc=$$?; \
	  docker compose down; \
	  exit $$rc

vet:
	go vet ./...

fmt-check:
	@diff=$$(gofmt -s -d . internal cmd); \
	  if [ -n "$$diff" ]; then \
	    echo "gofmt diff:"; \
	    echo "$$diff"; \
	    exit 1; \
	  fi

coverage:
	go test -cover ./internal/cimstomp/
