
# Wiggum Consumer Application

This project is a Python Kafka consumer application designed to process messages from a Kafka topic, extract content from web pages, and update a PostgreSQL database. The application connects to external Kafka and PostgreSQL services, with the consumer itself running in a Docker container for isolation and ease of deployment.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Environment Variables](#environment-variables)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Stopping the Application](#stopping-the-application)

## Prerequisites

- **Docker** and **Docker Compose** installed.
- Access to an external **Kafka** instance.
- Access to an external **PostgreSQL** instance.

## Environment Variables

The application requires certain environment variables to connect to external services. Create a `.env` file in the root directory with the following format:

```env
# PostgreSQL Configuration
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
POSTGRES_DB=your_db_name
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=5432

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=your_kafka_host:9092

# Proxy Configuration (for web scraping, if needed)
PROXY_URL=http://your_proxy_url:port
```

- **POSTGRES_USER**: Username for PostgreSQL.
- **POSTGRES_PASSWORD**: Password for PostgreSQL.
- **POSTGRES_DB**: Name of the PostgreSQL database.
- **POSTGRES_HOST**: Host address of the PostgreSQL server.
- **POSTGRES_PORT**: Port of the PostgreSQL server (default is `5432`).
- **KAFKA_BOOTSTRAP_SERVERS**: Kafka broker connection string (e.g., `your_kafka_host:9092`).
- **PROXY_URL**: Optional proxy URL for web scraping requests.

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/dannykellett/wiggum.git
   cd wiggum
   ```

2. **Create `.env` File**:
   Add the necessary environment variables in a `.env` file as shown above.

3. **Build and Run the Application**:
   Use Docker Compose to build and start the consumer application.
   ```bash
   docker-compose up --build -d
   ```

4. **Check Logs**:
   Verify that the application is running and connected to Kafka and PostgreSQL by checking the logs.
   ```bash
   docker-compose logs -f
   ```

## Usage

The consumer application listens to messages from the Kafka topic specified in the code. It processes each message by fetching HTML content from a provided URL, extracting the required text, updating the PostgreSQL database, and republishing the updated message to Kafka.

## Stopping the Application

To stop and remove the container, run:
```bash
docker-compose down
```

