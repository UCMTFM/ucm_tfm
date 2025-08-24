from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="databricks-telegram-bot",
    version="1.0.0",
    author="UCM TFM Team",
    author_email="your-email@example.com",
    description="A Telegram bot for querying Databricks Genie with natural language",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-repo/databricks-telegram-bot",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.11",
    install_requires=[
        "pytelegrambotapi>=4.14.0",
        "databricks-sdk>=0.12.0",
        "loguru>=0.7.1",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "databricks-genie-bot=databricks_telegram_bot.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "databricks_telegram_bot": ["env.example"],
    },
)
