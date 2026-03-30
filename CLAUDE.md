# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Starting the Services
- **Start Kafka and Zookeeper**:
  ```bash
  docker-compose up -d
  ```
- **Analytics PostgreSQL (Bronze/Silver/Gold)**:
 - Brought up by the same `docker-compose.yml` as service `flowguard-analytics`
 - Host port: `5434`

- **Start the Events Gateway**:
  - Method 1 (using CLI):
    ```bash
    python src/main.py start-gateway
    ```
  - Method 2 (direct uvicorn):
    ```bash
    python src/services/events_gateway/main.py
    ```
  - Method 3 (custom port):
    ```bash
    python src/main.py start-gateway --port 8001 --reload
    ```

### Health Checks
- After starting services, run:
  ```bash
  ./scripts/health_check.sh
  ```

### Installation
- **Python Dependencies**:
  ```bash
  source venv/bin/activate
  pip install -r requirements.txt
  ```

### Testing the API
- Verify API functionality via curl:
  ```bash
  curl http://localhost:8000/
  curl http://localhost:8000/health
  ```

## Architecture

### Overview
The `majorproject/` directory is structured for development in a microservices-like arrangement with the following key components:

- **Events Gateway** in `src/services/events_gateway/`:
  - Integrated with Kafka and built using FastAPI.
  - Contains routers for various event types (e.g., Orders, Clicks).

- **Shared Services** in `src/shared/`:
  - Shared configurations and schema definitions are included here, especially for Kafka interaction.

- **Infrastructure Configuration**:
  - **Docker**: `docker-compose.yml` sets up Kafka and Zookeeper.
  - **Environment**: `.env.example` for environment settings.

### Kafka and Zookeeper
Kafka cluster configuration, including brokers, is managed under the `config/` directory.

### Services
- **Order and Click Events**: REST API endpoints that interact with topics named `raw.orders.v1` and `raw.clicks.v1`.
- **Analytics Warehouse (local)**: PostgreSQL in Docker (`flowguard_analytics`) stores Bronze/Silver/Gold tables under schema `analytics`.

## Important Notes
- Ensure Kafka and Zookeeper are running before starting the gateway.
- Accessible UI routes include the Kafka UI at `http://localhost:8081` for monitoring.

Tools such as `curl` for testing API endpoints and Docker for managing the local infrastructure are central to the development process.

---
This guide should help future instances of Claude Code understand the flow and usage of core functionality within this codebase.
