# DataApp - Unified Business Intelligence Platform

## Overview
DataApp is a comprehensive data integration platform that unifies data from multiple business systems:
- Sitoo POS
- Shopify E-commerce

## Features
- Real-time data synchronization across all systems
- Unified data model for products, customers, orders, and inventory
- RESTful API for data access
- Comprehensive analytics and reporting
- Modern web dashboard

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- PostgreSQL 15+
- Redis 7+

### Installation
1. Clone the repository
2. Copy `env.example` to `.env` and configure your API keys
3. Run `docker-compose up -d`
4. Access the API at `http://localhost:8000`
5. Access the dashboard at `http://localhost:3000`

### Configuration
Update the `.env` file with your system credentials:
- Sitoo API key
- Shopify API credentials

## API Documentation
Once running, visit `http://localhost:8000/docs` for interactive API documentation.

## Data Models
- **Products**: Unified product catalog across all systems
- **Customers**: Centralized customer database
- **Orders**: Consolidated order management
- **Inventory**: Real-time inventory tracking

## Development
- Backend: FastAPI with SQLAlchemy
- Frontend: React with TypeScript
- Database: PostgreSQL
- Cache: Redis
- Orchestration: Apache Airflow (planned)

## License
MIT License
