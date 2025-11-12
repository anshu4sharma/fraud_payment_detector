# Fraud Payment Detector

A real-time payment fraud detection system built with Go, Kafka, and Redis. Detects anomalous transactions using simple, configurable rules.

## Core Rules

* High-Frequency Transactions – flags rapid successive transactions.

* Large Amounts – flags transactions exceeding thresholds.

* Geographical/Velocity Checks – flags impossible travel patterns between transactions.

## Quick Start

Clone the repo:

```bash
git clone git@github.com:anshu4sharma/fraud_payment_detector.git
cd fraud_payment_detector
```

Install dependencies:

```bash
go mod tidy
```
Start all services (Redis, Kafka, and the application) using Docker Compose:

```bash
docker-compose up -d
```

Run the application
```bash
go run main.go
```