.PHONY: generate build test clean

# Generate Go protobuf + Connect RPC stubs from sidecar.proto
generate:
	buf generate

# Build all packages
build: generate
	go build ./...

# Run tests
test: generate
	go test ./...

# Clean generated files
clean:
	rm -rf gen/
