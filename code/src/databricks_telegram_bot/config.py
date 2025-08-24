import os
from dataclasses import dataclass


@dataclass
class DatabricksConfig:
    workspace_url: str
    access_token: str
    catalog: str = "hive_metastore"
    schema: str = "default"


@dataclass
class TelegramConfig:
    bot_token: str
    allowed_users: list[str] = None


@dataclass
class AppConfig:
    databricks: DatabricksConfig
    telegram: TelegramConfig
    log_level: str = "INFO"
    max_query_timeout: int = 300  # seconds


def load_config() -> AppConfig:
    databricks_config = DatabricksConfig(
        workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL", ""),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN", ""),
        catalog=os.getenv("DATABRICKS_CATALOG", "hive_metastore"),
        schema=os.getenv("DATABRICKS_SCHEMA", "default"),
    )

    allowed_users_str = os.getenv("TELEGRAM_ALLOWED_USERS", "")
    allowed_users = [
        user.strip() for user in allowed_users_str.split(",") if user.strip()
    ]

    telegram_config = TelegramConfig(
        bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""), allowed_users=allowed_users
    )

    app_config = AppConfig(
        databricks=databricks_config,
        telegram=telegram_config,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        max_query_timeout=int(os.getenv("MAX_QUERY_TIMEOUT", "300")),
    )

    return app_config


def validate_config(config: AppConfig) -> None:
    """Validate that all required configuration is present"""
    if not config.databricks.workspace_url:
        raise ValueError("DATABRICKS_WORKSPACE_URL is required")

    if not config.databricks.access_token:
        raise ValueError("DATABRICKS_ACCESS_TOKEN is required")

    if not config.telegram.bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN is required")

    if not config.telegram.allowed_users:
        raise ValueError("TELEGRAM_ALLOWED_USERS is required")
