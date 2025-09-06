# UCM TFM Lakehouse Project

A comprehensive data lakehouse solution built for the UCM Big Data Master's program, featuring Databricks integration with multiple user interfaces for data exploration and a complete ETL data processing pipeline.

## 🏗️ Project Overview

This project consists of three main components:

1. **🤖 Databricks Telegram Bot** - Natural language data queries via Telegram
2. **🌐 Streamlit Web UI** - Interactive web interface for data exploration  
3. **⚙️ Data Processing Libraries** - Bronze and Silver layer ETL pipelines

## ✨ Features

### 🤖 Telegram Bot Interface
- **Natural Language Queries**: Ask questions in plain English about your data
- **SQL Generation**: Automatically generates SQL queries using Databricks Genie AI
- **Formatted Results**: Returns query results with proper number formatting (no scientific notation)
- **Table Discovery**: List and explore available tables in your Databricks workspace
- **User Authorization**: Secure access control with authorized user lists
- **Real-time Status**: Check bot and Databricks connection status
- **Error Handling**: Graceful error handling with user-friendly messages

### 🌐 Streamlit Web Interface
- **Interactive Chat Interface**: Modern web-based chat for data exploration
- **Visual Data Display**: Rich tables with proper number formatting and pagination
- **Chat History**: Persistent conversation history with expandable previous chats
- **Sample Queries**: Pre-built example queries specific to your data domain
- **Responsive Design**: Works on desktop and mobile devices
- **Real-time Results**: Live query execution with progress indicators
- **Smart Formatting**: Automatic data type detection and formatting

### ⚙️ Data Processing Libraries
- **Bronze Layer**: Raw data ingestion with batch and streaming support
- **Silver Layer**: Data transformation and cleaning processors
- **Modular Architecture**: Extensible registry-based component system
- **Error Handling**: Robust error handling and logging throughout the pipeline

### 🛠️ Shared Infrastructure
- **Comprehensive Logging**: Full logging with loguru for monitoring and debugging
- **Flexible Configuration**: Environment-based configuration management
- **Docker Support**: Containerized deployment with multi-service support
- **Testing Suite**: Comprehensive test coverage with pytest
- **Development Tools**: Modern tooling with uv package manager

## 🚀 Quick Start

### Prerequisites

- Python 3.11 or higher
- [uv](https://docs.astral.sh/uv/) package manager
- Databricks workspace with Genie enabled
- Telegram Bot Token (from [@BotFather](https://t.me/botfather)) - for Telegram bot only
- Telegram User ID (from [@userinfobot](https://t.me/userinfobot)) - for Telegram bot only

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd ucm_tfm_lakehouse
   ```

2. **Install dependencies using uv**:
   ```bash
   # Install uv if you haven't already
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Install project dependencies
   uv sync
   ```

3. **Configuration**:
   ```bash
   # Copy environment template
   cp .env.example .env
   # Edit .env with your credentials (see Configuration section below)
   ```

#### 🤖 Telegram Bot
```bash
# Using make command (recommended)
make run-bot

# Using uv directly
uv run -m src.databricks_telegram_bot.main

# With environment variables
source .env && make run-bot
```

#### 🌐 Streamlit Web UI
```bash
# Using make command (recommended)
make run-chat

# Using uv directly
uv run streamlit run src/databricks_telegram_bot/streamlit_ui.py --server.port 8501 --server.address 0.0.0.0

# With custom port
STREAMLIT_PORT=8080 make run-chat
```

Access the web interface at: http://localhost:8501

#### 🐳 Docker Deployment
```bash
# Build the universal Docker image
make build

# Run Telegram bot in Docker
make docker-run-bot

# Run Streamlit UI in Docker
make docker-run-chat

# Run with custom port
STREAMLIT_PORT=8080 make docker-run-chat
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file based on `env.example`:

| Variable | Required | Description | Default | Used By |
|----------|----------|-------------|---------|---------|
| `DATABRICKS_WORKSPACE_URL` | ✅ | Your Databricks workspace URL | - | Both |
| `DATABRICKS_ACCESS_TOKEN` | ✅ | Databricks personal access token | - | Both |
| `DATABRICKS_CATALOG` | ❌ | Data catalog name | `hive_metastore` | Both |
| `DATABRICKS_SCHEMA` | ❌ | Schema name | `default` | Both |
| `TELEGRAM_BOT_TOKEN` | ✅* | Bot token from @BotFather | - | Telegram Bot |
| `TELEGRAM_ALLOWED_USERS` | ✅* | Comma-separated user IDs | - | Telegram Bot |
| `LOG_LEVEL` | ❌ | Logging level | `INFO` | Both |
| `MAX_QUERY_TIMEOUT` | ❌ | Query timeout in seconds | `300` | Both |
| `STREAMLIT_PORT` | ❌ | Port for Streamlit UI | `8501` | Streamlit UI |

*Required only for Telegram bot functionality

### Getting Required Credentials

#### Databricks Access Token
1. Go to your Databricks workspace
2. Click on your profile → User Settings
3. Go to Access Tokens tab
4. Generate New Token
5. Copy the token to `DATABRICKS_ACCESS_TOKEN`

#### Telegram Bot Token
1. Message [@BotFather](https://t.me/botfather) on Telegram
2. Send `/newbot` command
3. Follow the prompts to create your bot
4. Copy the token to `TELEGRAM_BOT_TOKEN`

#### Telegram User ID
1. Message [@userinfobot](https://t.me/userinfobot) on Telegram
2. Copy your user ID to `TELEGRAM_ALLOWED_USERS`

## 🌐 Streamlit Web Interface

The Streamlit UI provides a modern, interactive web interface for data exploration.

### Features
- **📱 Responsive Design**: Works on desktop and mobile
- **🤖 Interactive Chat**: Real-time conversation with Genie AI
- **📊 Rich Data Display**: Tables, charts, and visualizations
- **📝 Smart History**: Last 2 chats expanded, others collapsed
- **💡 Sample Queries**: Click-to-use example questions
- **⚙️ Configuration Display**: Current workspace information
- **🔄 Auto-clear Input**: Input field clears after each question

### Usage
1. **Start the UI**: `source .env && make run-chat`
2. **Open Browser**: Navigate to http://localhost:8501
3. **Ask Questions**: Type natural language queries
4. **Explore Data**: View results in formatted tables
5. **Review History**: Access previous conversations

### Sample Interface
```
🤖 Databricks Genie Chat
Ask questions about your data in natural language!

💬 Ask a Question
┌─────────────────────────────────────────────┐
│ Show me the top 10 customers by revenue    │ [🚀 Ask]
└─────────────────────────────────────────────┘

📝 Chat History
▼ 🕐 14:32:15 - Show me the top 10 customers by revenue
   **Question:** Show me the top 10 customers by revenue
   **SQL Query:**
   SELECT customer_name, SUM(revenue) as total_revenue...
   
▼ 🕐 14:30:22 - What are total sales by month?
   **Question:** What are total sales by month?
   ...

▷ 🕐 14:28:18 - List all product categories
▷ 🕐 14:25:33 - Show me customer distribution
```

## 🤖 Telegram Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Welcome message and bot introduction |
| `/help` | Show available commands |
| `/tables` | List available tables in your Databricks workspace |
| `/status` | Check bot and Databricks connection status |

## 💬 Usage Examples

### Natural Language Queries (Improved Number Formatting)

```
User: Show me the top 10 customers by revenue this year

Bot: Here's your query result:

**SQL Query:**
SELECT customer_name, SUM(revenue) as total_revenue 
FROM sales_data 
WHERE year = 2025 
GROUP BY customer_name 
ORDER BY total_revenue DESC 
LIMIT 10

**Results:**
Customer Name | Total Revenue
---|---
Customer A | 8,524,412,042
Customer B | 7,876,196,538
Customer C | 6,400,955,374
Customer D | 2,158,492,105
Customer E | 1,023,456,789

**Explanation:**
This query finds the top 10 customers by total revenue in 2025, displaying results with proper number formatting instead of scientific notation.
```

### Table Discovery

```
User: /tables

Bot: 📋 **Available Tables:**

**Schema: Default**
• **Facturas** (`facturas`)
• **Detalle Facturas** (`detalle_facturas`)
• **Notas Credito** (`notas_credito`)
• **Detalle Notas Credito** (`detalle_notas_credito`)

You can ask questions about any of these tables using natural language!
```

### Sample Queries for UCM TFM Domain

```
"¿Cuáles son las 10 facturas con mayor valor total?"
"Muéstrame el total de ventas por mes en el año 2024"
"¿Cuáles son los productos más vendidos por cantidad?"
"Muéstrame las facturas que tienen saldo pendiente"
"¿Cuál es el total de IVA recaudado por mes?"
"Lista los clientes con mayor número de facturas"
"¿Cuáles son las facturas anuladas y sus motivos?"
"Muéstrame la tendencia de ventas por día de la semana"
```

## 🏗️ Architecture

```
                    ┌─────────────────┐
                    │   End Users     │
                    └─────────────────┘
                           │
                 ┌─────────┼─────────┐
                 │                   │
                 ▼                   ▼
    ┌─────────────────────┐  ┌─────────────────────┐
    │   Telegram Bot      │  │   Streamlit UI      │
    │   Interface         │  │   (Port 8501)       │
    └─────────────────────┘  └─────────────────────┘
                 │                   │
                 └─────────┬─────────┘
                           │
                           ▼
              ┌─────────────────────┐
              │ DatabricksGenie     │
              │ Client (Shared)     │
              └─────────────────────┘
                           │
                           ▼
              ┌─────────────────────┐
              │ Databricks          │
              │ Genie API           │
              └─────────────────────┘
                           │
                           ▼
    ┌─────────────────────────────────────────────────┐
    │              Data Lakehouse                     │
    │  ┌─────────────────┐  ┌─────────────────────┐   │
    │  │  Bronze Layer   │  │   Silver Layer      │   │
    │  │  (Raw Data)     │  │  (Processed Data)   │   │
    │  │                 │  │                     │   │
    │  │ • Batch Ingest  │  │ • Data Cleaning     │   │
    │  │ • Stream Ingest │  │ • Transformations   │   │
    │  │ • Error Handling│  │ • Business Logic    │   │
    │  └─────────────────┘  └─────────────────────┘   │
    └─────────────────────────────────────────────────┘
```

### Component Overview

#### Chat Interfaces
- **`main.py`**: Telegram bot entry point and lifecycle management
- **`telegram_bot.py`**: Telegram bot interface and message handling with improved formatting
- **`streamlit_ui.py`**: Streamlit web interface for interactive chat with enhanced UI
- **`databricks_client.py`**: Shared Databricks Genie API client with number formatting
- **`config.py`**: Configuration management and validation

#### Data Processing Libraries
- **`bronze_layer/`**: Raw data ingestion framework
  - `ingest_engine.py`: Main ingestion orchestrator
  - `registry.py`: Component registry for ingestors
  - `ingestors/`: Pluggable ingestion implementations
- **`silver_layer/`**: Data transformation framework
  - `process_engine.py`: Main processing orchestrator  
  - `registry.py`: Component registry for processors
  - `processors/`: Business logic processors for specific entities

#### Infrastructure
- **`Dockerfile`**: Universal container supporting both chat interfaces
- **`Makefile`**: Development and deployment automation
- **`pyproject.toml`**: Modern Python project configuration with uv

## 🐳 Docker Deployment

### Universal Docker Image

The project provides a single Docker image that can run both the Telegram bot and Streamlit UI using environment variables.

```bash
# Build the universal image
make build

# Run Telegram bot
source .env && make docker-run-bot

# Run Streamlit UI (default port 8501)
source .env && make docker-run-chat

# Run Streamlit UI on custom port
source .env && STREAMLIT_PORT=8080 make docker-run-chat

# Run with custom module
source .env && make docker-run-module MODULE=your.custom.module
```

### Available Make Commands

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make install` | Install dependencies using uv |
| `make test` | Run all tests with pytest |
| `make test-coverage` | Run tests with coverage report |
| `make build` | Build the universal Docker image |
| `make run-bot` | Run Telegram bot locally |
| `make run-chat` | Run Streamlit UI locally |
| `make docker-run-bot` | Run Telegram bot in container |
| `make docker-run-chat` | Run Streamlit UI in container |
| `make docker-run-module MODULE=x` | Run custom module in container |
| `make clean` | Clean up cache files and build artifacts |

### Direct Docker Commands

```bash
# Run Telegram bot
docker run --rm -it \
  -e DATABRICKS_WORKSPACE_URL=your-url \
  -e DATABRICKS_ACCESS_TOKEN=your-token \
  -e TELEGRAM_BOT_TOKEN=your-bot-token \
  -e TELEGRAM_ALLOWED_USERS=user-ids \
  -e ENTRYPOINT_MODULE=src.databricks_telegram_bot.main \
  ucm-tfm-lakehouse:latest

# Run Streamlit UI
docker run --rm -it \
  -p 8501:8501 \
  -e DATABRICKS_WORKSPACE_URL=your-url \
  -e DATABRICKS_ACCESS_TOKEN=your-token \
  --entrypoint sh \
  ucm-tfm-lakehouse:latest \
  -c "uv run streamlit run src/databricks_telegram_bot/streamlit_ui.py --server.port 8501 --server.address 0.0.0.0"
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage
```

### Test Structure

```
tests/
├── databricks_telegram_bot/
│   ├── test_databricks_client.py    # Databricks API client tests
│   └── test_telegram_bot.py         # Telegram bot functionality tests
```

## 🛠️ Development

### Dependency Management

This project uses [uv](https://docs.astral.sh/uv/) for fast and reliable dependency management:

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync

# Add a new dependency
uv add package-name

# Add a development dependency  
uv add --dev package-name

# Update dependencies
uv lock --upgrade

# Run commands in the project environment
uv run python src/databricks_telegram_bot/main.py
```

### Development Workflow

```bash
# Set up development environment
make install

# Run components locally
make run-bot          # Start Telegram bot
make run-chat         # Start Streamlit UI

# Run tests
make test             # Run all tests
make test-coverage    # Run tests with coverage

# Build and deploy
make build            # Build Docker image
make docker-run-bot   # Test bot in container
make docker-run-chat  # Test UI in container

# Clean up
make clean            # Remove cache and build artifacts
```

## 📊 Data Processing Libraries

The project includes a complete ETL framework for data lakehouse operations:

### Bronze Layer (Raw Data Ingestion)
- **Batch Ingestor**: Handles batch data loading from various sources
- **Streaming Ingestor**: Real-time data streaming capabilities
- **Registry System**: Pluggable architecture for different data sources
- **Error Handling**: Robust error handling and retry mechanisms

### Silver Layer (Data Transformation)
- **Business Logic Processors**: Domain-specific transformations
  - `facturas.py`: Invoice data processing
  - `detalle_facturas.py`: Invoice detail processing  
  - `notas_credito.py`: Credit note processing
  - `detalle_notas_credito.py`: Credit note detail processing
- **Data Quality**: Validation and cleaning operations
- **Extensible Framework**: Easy to add new business logic

*Built with ❤️ for the UCM Big Data Master's Program*


