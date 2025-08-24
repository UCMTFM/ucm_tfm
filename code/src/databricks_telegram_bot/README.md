# Databricks Telegram Bot

A Telegram bot that provides natural language interface to Databricks using Genie AI. Ask questions in plain English and get SQL queries, results, and explanations directly in your Telegram chat.

## ✨ Features

- **Natural Language Queries**: Ask questions in plain English about your data
- **SQL Generation**: Automatically generates SQL queries using Databricks Genie
- **Data Results**: Returns query results formatted for Telegram
- **Table Discovery**: List and explore available tables in your Databricks workspace
- **User Authorization**: Secure access control with authorized user lists
- **Real-time Status**: Check bot and Databricks connection status
- **Error Handling**: Graceful error handling with user-friendly messages
- **Logging**: Comprehensive logging for monitoring and debugging

## 🚀 Quick Start

### Prerequisites

- Python 3.11 or higher
- [uv](https://docs.astral.sh/uv/) package manager
- Databricks workspace with Genie enabled
- Telegram Bot Token (from [@BotFather](https://t.me/botfather))
- Telegram User ID (from [@userinfobot](https://t.me/userinfobot))

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
   
   # Install project dependencies
   uv sync
   ```

3. **Configuration**:
   ```bash
   cd src/databricks_telegram_bot
   cp env.example .env
   # Edit .env with your credentials
   ```

4. **Run the bot**:
   ```bash
   # From the code/ directory
   uv run python src/databricks_telegram_bot/main.py
   ```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file based on `env.example`:

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `DATABRICKS_WORKSPACE_URL` | ✅ | Your Databricks workspace URL | - |
| `DATABRICKS_ACCESS_TOKEN` | ✅ | Databricks personal access token | - |
| `DATABRICKS_CATALOG` | ❌ | Data catalog name | `hive_metastore` |
| `DATABRICKS_SCHEMA` | ❌ | Schema name | `default` |
| `TELEGRAM_BOT_TOKEN` | ✅ | Bot token from @BotFather | - |
| `TELEGRAM_ALLOWED_USERS` | ✅ | Comma-separated user IDs | - |
| `LOG_LEVEL` | ❌ | Logging level | `INFO` |
| `MAX_QUERY_TIMEOUT` | ❌ | Query timeout in seconds | `300` |

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

## 🤖 Bot Commands

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
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Telegram User  │◄──►│ Telegram Bot API │◄──►│ DatabricksTele- │
└─────────────────┘    └──────────────────┘    │   gramBot       │
                                               └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │ DatabricksGenie │
                                               │    Client       │
                                               └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │ Databricks      │
                                               │ Genie API       │
                                               └─────────────────┘
```

### Components

- **`main.py`**: Application entry point and lifecycle management
- **`telegram_bot.py`**: Telegram bot interface and message handling
- **`databricks_client.py`**: Databricks Genie API client
- **`config.py`**: Configuration management and validation

## 🐳 Docker Deployment

### Build and Run

```bash
# Build the image (from code/ directory)
docker build -f src/databricks_telegram_bot/Dockerfile -t databricks-telegram-bot .

# Run with environment file
docker run -d --name databricks-bot --env-file src/databricks_telegram_bot/.env databricks-telegram-bot
```

### Docker Compose

```bash
# From src/databricks_telegram_bot/ directory
cd src/databricks_telegram_bot

# Start the bot
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the bot
docker-compose down
```

## 🧪 Testing

### Running Tests

```bash
# From the code/ directory
# Install test dependencies (if not already installed)
uv add --dev pytest pytest-mock pytest-cov

# Run all tests
uv run pytest test/databricks_telegram_bot/

# Run with coverage
uv run pytest test/databricks_telegram_bot/ --cov=src/databricks_telegram_bot --cov-report=html

# Run specific test file
uv run pytest test/databricks_telegram_bot/test_config.py -v
```

### Test Structure

```
test/
├── databricks_telegram_bot/
│   ├── test_config.py           # Configuration tests
│   ├── test_databricks_client.py # Databricks client tests
│   ├── test_telegram_bot.py     # Telegram bot tests
│   └── test_main.py             # Main application tests
├── conftest.py                  # Shared fixtures
└── pytest.ini                  # Test configuration
```

## 📊 Monitoring and Logging

### Log Files

- **Console**: Real-time logs with INFO level
- **File**: `logs/bot.log` with DEBUG level (rotated daily)
- **Errors**: Critical errors logged with full stack traces

### Log Levels

- `DEBUG`: Detailed information for debugging
- `INFO`: General information about bot operations
- `WARNING`: Warning messages about potential issues
- `ERROR`: Error messages for failed operations

### Health Monitoring

The bot provides several endpoints for monitoring:

- **PID File**: `bot.pid` for process management
- **Status Command**: `/status` for connection health
- **Graceful Shutdown**: Signal handling for clean stops

## 🔒 Security Considerations

### Access Control
- Only authorized Telegram users can interact with the bot
- User IDs are validated on every message
- Unauthorized access attempts are logged

### Data Protection
- No data is stored permanently by the bot
- Query results are only sent to authorized users
- Sensitive configuration is loaded from environment variables

### Network Security
- All communications use HTTPS/TLS
- Databricks tokens are securely transmitted
- No credentials are logged or exposed

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
├── pyproject.toml            # Project configuration and dependencies
├── uv.lock                   # Locked dependency versions
├── src/
│   └── databricks_telegram_bot/
│       ├── __init__.py
│       ├── main.py           # Application entry point
│       ├── config.py         # Configuration management
│       ├── databricks_client.py # Databricks API client
│       ├── telegram_bot.py   # Telegram bot implementation
│       ├── env.example       # Environment template
│       ├── Dockerfile        # Docker configuration
│       ├── docker-compose.yml # Docker Compose setup
│       ├── setup.py          # Package setup
│       └── logs/             # Log directory
└── test/
    └── databricks_telegram_bot/ # Test files
```

### Adding Features

1. **New Commands**: Add handlers in `telegram_bot.py`
2. **Databricks Features**: Extend `databricks_client.py`
3. **Configuration**: Update `config.py` and `env.example`
4. **Tests**: Add corresponding test files

### Code Style

- **Formatting**: Use `black` for code formatting
- **Linting**: Use `flake8` for code linting
- **Type Hints**: Use type hints for better code documentation
- **Docstrings**: Document all public functions and classes
