# Databricks Genie Telegram Bot

A Telegram bot that integrates with Databricks Genie to provide natural language query capabilities for your data lakehouse. Users can ask questions in plain English and receive formatted results directly in Telegram.

## Features

- **Natural Language Queries**: Ask questions in plain English about your data
- **Databricks Genie Integration**: Leverages Databricks Genie API for intelligent query generation
- **Telegram Bot Interface**: Easy-to-use chat interface
- **Synchronous Architecture**: Simple, reliable synchronous operations
- **Table Discovery**: Browse available tables with `/tables` command
- **Formatted Results**: Clean, readable query results with SQL preview
- **User Authorization**: Configurable user access control

## Architecture

### Core Components

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Telegram Bot  │───▶│  Databricks      │───▶│  Databricks     │
│                 │    │  Genie Client    │    │  Genie API      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌──────────────────┐
│   User Input    │    │  Configuration   │
│   Processing    │    │  Management      │
└─────────────────┘    └──────────────────┘
```

### Technology Stack

- Python 3.11+
- Databricks workspace with Genie enabled
- Telegram bot token
- Databricks access token

## Installation

### Prerequisites

1. **Clone and navigate to the bot directory**:
   ```bash
   cd code/src/databricks_telegram_bot
   ```

2. **Run the deployment script**:
   ```bash
   ./deploy.sh
   ```

The script will:
- Check Python version
- Create virtual environment
- Install dependencies
- Validate configuration
- Start the bot

### Manual Installation

1. **Set up environment**:
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

2. **Create virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the bot**:
   ```bash
   python -m databricks_telegram_bot.main
   ```

## ⚙️ Configuration

Create a `.env` file with the following variables:

```bash
# Databricks Configuration
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
DATABRICKS_ACCESS_TOKEN=your_databricks_token
DATABRICKS_CATALOG=hive_metastore
DATABRICKS_SCHEMA=default

# Telegram Configuration
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_ALLOWED_USERS=123456789,987654321

# Application Configuration
LOG_LEVEL=INFO
MAX_QUERY_TIMEOUT=300
```

## Usage

### Starting the Bot

```bash
python -m src.databricks_telegram_bot.main
```

### Bot Commands

- `/start` - Initialize the bot and check authorization
- `/tables` - List available tables in your catalog/schema
- `/help` - Show available commands

### Example Queries

- "What is our highest total value invoice?"
- "Show me the top 5 customers by sales"
- "How many invoices do we have from last month?"
- "What's the average order value?"

## Configuration

### Databricks Setup

1. **Enable Genie**: Ensure Genie is enabled in your Databricks workspace
2. **Permissions**: Your token needs access to:
   - SQL warehouses
   - Unity Catalog (if using)
   - Genie API
3. **Workspace URL**: Use your workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)

### Telegram Setup

1. **Create Bot**: Message @BotFather on Telegram
2. **Get Token**: Save the bot token provided
3. **Authorize Users**: Add user IDs to `TELEGRAM_AUTHORIZED_USERS`

## Development

### Project Structure

```
src/databricks_telegram_bot/
├── __init__.py
├── main.py                 # Application entry point
├── config.py              # Configuration management
├── databricks_client.py   # Databricks Genie client
├── telegram_bot.py        # Telegram bot implementation
├── health_check.py        # Health monitoring
└── setup.py              # Package configuration
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest test/test_databricks_client.py

# Run with coverage
pytest --cov=src.databricks_telegram_bot

# Run specific test class
pytest test/test_databricks_client.py::TestDatabricksGenieClient
```

### Testing Structure

Tests are organized by source file with pytest classes:

- `test_databricks_client.py` - Tests for DatabricksGenieClient
- `test_telegram_bot.py` - Tests for DatabricksTelegramBot
- `test_main.py` - Tests for main application
- `test_config.py` - Tests for configuration management

## API Reference

### DatabricksGenieClient

Main client for interacting with Databricks Genie API.

```python
from src.databricks_telegram_bot.databricks_client import DatabricksGenieClient

client = DatabricksGenieClient(config)

# Send natural language query
response = client.query_genie("What is our highest sale?")

# Get available tables
tables = client.get_available_tables()
```

### DatabricksTelegramBot

Telegram bot implementation using pytelegrambotapi.

```python
from src.databricks_telegram_bot.telegram_bot import DatabricksTelegramBot

bot = DatabricksTelegramBot(config, databricks_client)
bot.start()  # Start polling
```

## Troubleshooting

### Common Issues

1. **"Query timed out"**: Increase timeout in configuration or check warehouse status
2. **"Unauthorized user"**: Add user ID to `TELEGRAM_AUTHORIZED_USERS`
3. **"No tables found"**: Check catalog/schema configuration and permissions
4. **"Genie API error"**: Verify Genie is enabled and token has proper permissions

### Logging

The application uses structured logging with loguru. Log levels can be configured via `LOG_LEVEL`:

- `DEBUG`: Detailed debugging information
- `INFO`: General application flow
- `WARNING`: Warning messages
- `ERROR`: Error conditions

### Health Check

Monitor bot health with the built-in health check:

```python
from src.databricks_telegram_bot.health_check import HealthChecker

checker = HealthChecker(config)
status = checker.check_health()
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs for error details
3. Open an issue on GitHub with relevant information

