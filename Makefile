test: install
	go test -mod=readonly ./...

test-race: | test
	go test -race -mod=readonly ./...

vet: | test
	go vet ./...

install:
	go install ./cmd/azure-log/... -o azure-log

run:
	go run ./cmd/azure-log/azure-log.go ./cmd/azure-log/prometheus_util.go

build:
	go build -o bin/azure-log.exe ./cmd/azure-log/...

fmt:
	go fmt -mod=readonly ./cmd/azure-log/...

download:
	go mod tidy
	go mod download
