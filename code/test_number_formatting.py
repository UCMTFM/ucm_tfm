#!/usr/bin/env python3

"""Test script to verify number formatting in the Databricks client"""

import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from databricks_telegram_bot.databricks_client import DatabricksGenieClient
from databricks_telegram_bot.config import DatabricksConfig

def test_number_formatting():
    """Test the number formatting methods"""
    
    # Create a dummy config for testing
    config = DatabricksConfig(
        workspace_url="https://dummy.databricks.com",
        access_token="dummy",
        catalog="test",
        schema="test"
    )
    
    # Create client instance (we only need it for the formatting methods)
    client = DatabricksGenieClient(config)
    
    # Test values similar to your example
    test_values = [
        "8.524412042279774E9",  # Scientific notation from your example
        8.524412042279774E9,    # Actual float value
        8358075570549746,       # Large integer
        6.400955373870109E9,    # Another scientific notation
        123456789,              # Regular large number
        1234.567,               # Decimal number
        42,                     # Small integer
        0.0123,                 # Small decimal
        "invalid_number",       # Non-numeric string
    ]
    
    print("Testing number formatting:")
    print("=" * 50)
    
    for value in test_values:
        try:
            is_numeric = client.is_numeric_value(value)
            if is_numeric:
                formatted = client.format_number_for_display(value)
                print(f"Input: {value}")
                print(f"Type: {type(value)}")
                print(f"Formatted: {formatted}")
                print(f"Is numeric: {is_numeric}")
                print("-" * 30)
            else:
                print(f"Input: {value}")
                print(f"Type: {type(value)}")
                print(f"Not numeric, skipping formatting")
                print("-" * 30)
        except Exception as e:
            print(f"Error processing {value}: {e}")
            print("-" * 30)
    
    # Test a sample response formatting similar to your Telegram output
    print("\nTesting response formatting:")
    print("=" * 50)
    
    sample_response = {
        "query": "Show me total sales by month",
        "sql_query": "SELECT Mes, SUM(TotalVentas) as TotalVentas FROM sales GROUP BY Mes",
        "result": {
            "data": [
                [12, 8.524412042279774E9],
                [10, 8.358075570549746E9],
                [1, 6.400955373870109E9],
                [3, 7.323935117650145E9],
            ],
            "columns": ["Mes", "TotalVentas"],
            "summary": "Query returned 4 rows"
        },
        "explanation": "Here are the total sales by month"
    }
    
    formatted_response = client.format_genie_response(sample_response)
    print("Formatted Response:")
    print(formatted_response)

if __name__ == "__main__":
    test_number_formatting()
