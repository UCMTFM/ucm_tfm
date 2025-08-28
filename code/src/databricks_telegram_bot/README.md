# Databricks Telegram Bot & Chat UI

A comprehensive solution providing multiple interfaces to interact with Databricks using Genie AI. Ask questions in plain English and get SQL queries, results, and explanations through Telegram or a modern web interface.

## ✨ Features

### 🤖 Telegram Bot
- **Natural Language Queries**: Ask questions in plain English about your data
- **SQL Generation**: Automatically generates SQL queries using Databricks Genie
- **Data Results**: Returns query results formatted for Telegram
- **Table Discovery**: List and explore available tables in your Databricks workspace
- **User Authorization**: Secure access control with authorized user lists
- **Real-time Status**: Check bot and Databricks connection status

### 🌐 Streamlit Web UI
- **Interactive Chat Interface**: Modern web-based chat for data exploration
- **Visual Data Display**: Rich tables, charts, and data visualization
- **Chat History**: Persistent conversation history with expandable previous chats
- **Sample Queries**: Pre-built example queries to get started quickly
- **Responsive Design**: Works on desktop and mobile devices
- **Real-time Results**: Live query execution with progress indicators

### 🛠️ Shared Features
- **Error Handling**: Graceful error handling with user-friendly messages
- **Comprehensive Logging**: Full logging for monitoring and debugging
- **Flexible Configuration**: Environment-based configuration
- **Docker Support**: Containerized deployment options

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
   cd code
   ```

2. **Install dependencies using uv**:
   ```bash
   # Install uv if you haven't already
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Install project dependencies (includes Streamlit)
   uv sync
   ```

3. **Configuration**:
   ```bash
   # Copy environment template
   cp .env.example .env
   # Edit .env with your credentials
   ```

### Running the Applications

#### 🤖 Telegram Bot
```bash
# From the project root
source .env && make run-bot
# or
uv run -m src.databricks_telegram_bot.main
```

#### 🌐 Streamlit Web UI
```bash
# From the project root
source .env && make run-chat
# or
source .env && uv run streamlit run src/databricks_telegram_bot/streamlit_ui.py --server.port 8501 --server.address 0.0.0.0
```

Access the web interface at: http://localhost:8501

#### 🐳 Docker Options
```bash
# Build the universal image
make build

# Run Telegram bot in Docker
source .env && make docker-run-bot

# Run Streamlit UI in Docker
source .env && make docker-run-chat

# Custom port for Streamlit
source .env && STREAMLIT_PORT=8080 make docker-run-chat
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

### Natural Language Queries

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
Customer A: $125,000
Customer B: $98,500
...

**Explanation:**
This query finds the top 10 customers by total revenue in 2025 by summing revenue per customer and ordering by total revenue in descending order.
```

### Table Discovery

```
User: /tables

Bot: Available Tables in your workspace:

📊 **sales_data** (default.sales_data)
- Schema: default
- Full name: hive_metastore.default.sales_data

📊 **customer_info** (default.customer_info)  
- Schema: default
- Full name: hive_metastore.default.customer_info

📊 **product_catalog** (default.product_catalog)
- Schema: default  
- Full name: hive_metastore.default.product_catalog
```

## 🏗️ Architecture

```
                    ┌─────────────────┐
                    │  End Users      │
                    └─────────────────┘
                           │
                           ▼
     ┌─────────────────────────────────────────────────┐
     │                                                 │
     ▼                                                 ▼
┌─────────────────┐                          ┌─────────────────┐
│ Telegram Bot    │                          │ Streamlit UI    │
│ Interface       │                          │ (Port 8501)     │
└─────────────────┘                          └─────────────────┘
     │                                                 │
     └─────────────────┐     ┌─────────────────────────┘
                       │     │
                       ▼     ▼
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
```

### Components

- **`main.py`**: Application entry point and lifecycle management
- **`telegram_bot.py`**: Telegram bot interface and message handling
- **`streamlit_ui.py`**: Streamlit web interface for interactive chat
- **`databricks_client.py`**: Shared Databricks Genie API client
- **`config.py`**: Configuration management and validation
- **`Dockerfile`**: Universal container supporting both interfaces
- **`Makefile`**: Development and deployment commands

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
| `make build` | Build the universal Docker image |
| `make docker-run-bot` | Run Telegram bot in container |
| `make docker-run-chat` | Run Streamlit UI in container |
| `make docker-run-bash` | Open bash shell in container |
| `make docker-run-module MODULE=x` | Run custom module |
| `make test` | Run all tests |
| `make clean` | Clean up cache files |

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
  ucm-tfm-lakehouse:latest \
  streamlit run src/databricks_telegram_bot/streamlit_ui.py --server.port 8501 --server.address 0.0.0.0
```

## 🧪 Testing

### Running Tests

```bash
# From the project root directory
# Run all tests
make test

# Run with coverage
make test-coverage

# Run tests in Docker
source .env && make docker-run-module MODULE=pytest

# Run specific test file
uv run pytest tests/databricks_telegram_bot/test_config.py -v
```

### Test Structure

```
tests/
├── databricks_telegram_bot/
│   ├── test_config.py           # Configuration tests
│   ├── test_databricks_client.py # Databricks client tests
│   ├── test_telegram_bot.py     # Telegram bot tests
│   └── test_main.py             # Main application tests
├── conftest.py                  # Shared fixtures
└── pytest.ini                  # Test configuration
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

### Project Structure

```
code/                          # Root project directory
├── Makefile                  # Development and deployment commands
├── Dockerfile               # Universal container configuration
├── pyproject.toml           # Project configuration and dependencies
├── uv.lock                  # Locked dependency versions
├── .env                     # Environment configuration
├── src/
│   └── databricks_telegram_bot/
│       ├── __init__.py
│       ├── main.py          # Telegram bot entry point
│       ├── streamlit_ui.py  # Streamlit web interface
│       ├── config.py        # Configuration management
│       ├── databricks_client.py # Databricks API client
│       ├── telegram_bot.py  # Telegram bot implementation
│       └── logs/            # Log directory
├── tests/
│   └── databricks_telegram_bot/ # Test files
└── logs/                    # Application logs
```

### Development Workflow

```bash
# Set up development environment
make install

# Run Telegram bot locally
source .env && make run-bot

# Run Streamlit UI locally  
source .env && make run-chat

# Run tests
make test

# Build Docker image
make build

# Clean up
make clean
``
