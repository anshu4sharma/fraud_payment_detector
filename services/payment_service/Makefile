# Project metadata
APP_NAME := fraud_payment_detector
PKG := github.com/anshu4sharma/$(APP_NAME)
BIN_DIR := bin
BUILD_DIR := build
GO_FILES := $(shell find . -name '*.go' -not -path "./vendor/*")

# Go parameters
GO := go
GO_FLAGS := -mod=readonly
LDFLAGS := -s -w

# Run service locally
.PHONY: run
run:
	@echo "🚀 Starting $(APP_NAME)..."
	$(GO) run ./cmd

# Format code
.PHONY: fmt
fmt:
	@echo "🧹 Formatting Go code..."
	$(GO) fmt ./...

# Tidy modules
.PHONY: tidy
tidy:
	@echo "📦 Tidying modules..."
	$(GO) mod tidy
