import pytest
from unittest.mock import Mock
from databricks_telegram_bot.databricks_client import DatabricksGenieClient
from databricks_telegram_bot.config import DatabricksConfig


class TestDatabricksGenieClientInitialization:
    def test_client_initializes_with_valid_config(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com",
            access_token="test_token_123",
            catalog="test_catalog",
            schema="test_schema",
        )
        workspace_client_mock = Mock()
        genie_api_mock = Mock()

        # When
        client = DatabricksGenieClient(config)
        client.client = workspace_client_mock
        client.genie_api = genie_api_mock

        # Then
        assert client.config == config
        assert client.client == workspace_client_mock
        assert client.genie_api == genie_api_mock
        assert client.catalog == "test_catalog"
        assert client.schema == "test_schema"
        assert client.space_id is None
        assert client.conversation_id is None

    def test_client_stores_configuration_properties_correctly(self):
        # Given
        expected_url = "https://production.databricks.com"
        expected_token = "dapi123456789abcdef"
        expected_catalog = "production_catalog"
        expected_schema = "production_schema"

        config = DatabricksConfig(
            workspace_url=expected_url,
            access_token=expected_token,
            catalog=expected_catalog,
            schema=expected_schema,
        )

        # When
        client = DatabricksGenieClient(config)

        # Then
        assert client.config.workspace_url == expected_url
        assert client.config.access_token == expected_token
        assert client.config.catalog == expected_catalog
        assert client.config.schema == expected_schema


class TestDatabricksGenieClientSpaceManagement:
    def test_get_or_create_space_returns_existing_space_when_available(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        space_mock = Mock()
        space_mock.space_id = "space_12345"

        spaces_response_mock = Mock()
        spaces_response_mock.spaces = [space_mock]

        genie_api_mock = Mock()
        genie_api_mock.list_spaces.return_value = spaces_response_mock

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock

        # When
        space_id = client._get_or_create_space()

        # Then
        assert space_id == "space_12345"
        assert client.space_id == "space_12345"
        genie_api_mock.list_spaces.assert_called_once()

    def test_get_or_create_space_returns_cached_space_when_already_set(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        genie_api_mock = Mock()

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.space_id = "cached_space_789"

        # When
        space_id = client._get_or_create_space()

        # Then
        assert space_id == "cached_space_789"
        genie_api_mock.list_spaces.assert_not_called()

    def test_get_or_create_space_raises_exception_when_no_spaces_available(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        spaces_response_mock = Mock()
        spaces_response_mock.spaces = []

        genie_api_mock = Mock()
        genie_api_mock.list_spaces.return_value = spaces_response_mock

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock

        # When & Then
        with pytest.raises(Exception, match="No Genie spaces available"):
            client._get_or_create_space()

    def test_get_or_create_space_handles_api_error_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        genie_api_mock = Mock()
        genie_api_mock.list_spaces.side_effect = Exception("API connection failed")

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock

        # When & Then
        with pytest.raises(Exception, match="API connection failed"):
            client._get_or_create_space()


class TestDatabricksGenieClientConversationManagement:
    def test_get_or_create_conversation_starts_new_conversation_when_none_exists(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        conversation_response_mock = Mock()
        conversation_response_mock.conversation_id = "conv_12345"

        genie_api_mock = Mock()
        genie_api_mock.start_conversation_and_wait.return_value = (
            conversation_response_mock
        )

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock

        # When
        conversation_id = client._get_or_create_conversation("space_123")

        # Then
        assert conversation_id == "conv_12345"
        assert client.conversation_id == "conv_12345"
        genie_api_mock.start_conversation_and_wait.assert_called_once_with(
            space_id="space_123", content="Hello, I'm ready to help with data queries."
        )

    def test_get_or_create_conversation_returns_cached_conversation_when_already_set(
        self,
    ):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        genie_api_mock = Mock()

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.conversation_id = "cached_conv_789"

        # When
        conversation_id = client._get_or_create_conversation("space_123")

        # Then
        assert conversation_id == "cached_conv_789"
        genie_api_mock.start_conversation_and_wait.assert_not_called()

    def test_get_or_create_conversation_handles_api_error_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        genie_api_mock = Mock()
        genie_api_mock.start_conversation_and_wait.side_effect = Exception(
            "Conversation creation failed"
        )

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock

        # When & Then
        with pytest.raises(Exception, match="Conversation creation failed"):
            client._get_or_create_conversation("space_123")


class TestDatabricksGenieClientQueryExecution:
    def test_query_genie_executes_simple_question_successfully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        message_response_mock = Mock()
        message_response_mock.content = "Here are the results for your query"
        message_response_mock.attachments = []

        genie_api_mock = Mock()
        genie_api_mock.create_message_and_wait.return_value = message_response_mock

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.space_id = "space_123"
        client.conversation_id = "conv_456"

        # When
        result = client.query_genie("Show me total sales")

        # Then
        assert result["query"] == "Show me total sales"
        assert result["explanation"] == "Here are the results for your query"
        assert result["sql_query"] == ""
        assert result["result"]["data"] == []
        assert result["result"]["summary"] == "Query processed by Genie"

        genie_api_mock.create_message_and_wait.assert_called_once_with(
            space_id="space_123",
            conversation_id="conv_456",
            content="Show me total sales",
        )

    def test_query_genie_processes_message_with_text_attachments(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        text_attachment_mock = Mock()
        text_attachment_mock.text = Mock()
        text_attachment_mock.text.content = (
            "This query shows sales data from the last quarter"
        )

        message_response_mock = Mock()
        message_response_mock.content = "Query processed"
        message_response_mock.attachments = [text_attachment_mock]

        genie_api_mock = Mock()
        genie_api_mock.create_message_and_wait.return_value = message_response_mock

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.space_id = "space_123"
        client.conversation_id = "conv_456"

        # When
        result = client.query_genie("Show me sales data")

        # Then
        assert result["query"] == "Show me sales data"
        assert (
            result["explanation"] == "This query shows sales data from the last quarter"
        )

    def test_query_genie_processes_message_with_query_attachments(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        query_attachment_mock = Mock()
        query_attachment_mock.attachment_id = "attach_123"
        query_attachment_mock.query = Mock()
        query_attachment_mock.query.query = (
            "SELECT * FROM sales WHERE date > '2024-01-01'"
        )

        message_response_mock = Mock()
        message_response_mock.id = "msg_789"
        message_response_mock.content = "Query executed"
        message_response_mock.attachments = [query_attachment_mock]

        query_result_mock = Mock()
        query_result_mock.statement_response = Mock()
        query_result_mock.statement_response.status = Mock()
        query_result_mock.statement_response.status.state = Mock()
        query_result_mock.statement_response.status.state.value = "SUCCEEDED"
        query_result_mock.statement_response.result = Mock()
        query_result_mock.statement_response.result.data_array = [
            ["Product A", 1000],
            ["Product B", 2000],
        ]
        query_result_mock.statement_response.result.manifest = Mock()
        query_result_mock.statement_response.result.manifest.schema = Mock()

        column_mock_1 = Mock()
        column_mock_1.name = "product_name"
        column_mock_2 = Mock()
        column_mock_2.name = "sales_amount"
        query_result_mock.statement_response.result.manifest.schema.columns = [
            column_mock_1,
            column_mock_2,
        ]

        genie_api_mock = Mock()
        genie_api_mock.create_message_and_wait.return_value = message_response_mock
        genie_api_mock.execute_message_attachment_query.return_value = query_result_mock

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.space_id = "space_123"
        client.conversation_id = "conv_456"

        # When
        result = client.query_genie("Show me product sales")

        # Then
        assert result["query"] == "Show me product sales"
        assert result["sql_query"] == "SELECT * FROM sales WHERE date > '2024-01-01'"
        assert result["result"]["data"] == [["Product A", 1000], ["Product B", 2000]]
        assert result["result"]["columns"] == ["product_name", "sales_amount"]
        assert result["result"]["summary"] == "Query returned 2 rows"

        genie_api_mock.execute_message_attachment_query.assert_called_once_with(
            space_id="space_123",
            conversation_id="conv_456",
            message_id="msg_789",
            attachment_id="attach_123",
        )

    def test_query_genie_handles_query_execution_error_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        query_attachment_mock = Mock()
        query_attachment_mock.attachment_id = "attach_123"
        query_attachment_mock.query = "SELECT * FROM non_existent_table"

        message_response_mock = Mock()
        message_response_mock.content = "Query failed"
        message_response_mock.attachments = [query_attachment_mock]

        genie_api_mock = Mock()
        genie_api_mock.create_message_and_wait.return_value = message_response_mock
        genie_api_mock.execute_message_attachment_query.side_effect = Exception(
            "Table does not exist"
        )

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.space_id = "space_123"
        client.conversation_id = "conv_456"

        # When
        result = client.query_genie("Show me data from invalid table")

        # Then
        assert result["query"] == "Show me data from invalid table"
        assert (
            result["result"]["summary"] == "Error executing query: Table does not exist"
        )

    def test_query_genie_handles_api_error_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        genie_api_mock = Mock()
        genie_api_mock.create_message_and_wait.side_effect = Exception(
            "API request failed"
        )

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.space_id = "space_123"
        client.conversation_id = "conv_456"

        # When & Then
        with pytest.raises(Exception, match="API request failed"):
            client.query_genie("Show me data")


class TestDatabricksGenieClientQueryStatus:
    def test_get_query_status_returns_status_for_valid_query_id(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        message_mock = Mock()
        message_mock.status = "COMPLETED"
        message_mock.content = "Query executed successfully"

        genie_api_mock = Mock()
        genie_api_mock.get_message.return_value = message_mock

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.space_id = "space_123"
        client.conversation_id = "conv_456"

        # When
        status = client.get_query_status("query_789")

        # Then
        assert status["query_id"] == "query_789"
        assert status["status"] == "COMPLETED"
        assert status["message"] == "Query executed successfully"

        genie_api_mock.get_message.assert_called_once_with(
            space_id="space_123", conversation_id="conv_456", message_id="query_789"
        )

    def test_get_query_status_returns_unknown_when_no_active_conversation(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        genie_api_mock = Mock()

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        # No space_id or conversation_id set

        # When
        status = client.get_query_status("query_789")

        # Then
        assert status["query_id"] == "query_789"
        assert status["status"] == "UNKNOWN"
        assert status["message"] == "No active conversation"

        genie_api_mock.get_message.assert_not_called()

    def test_get_query_status_handles_api_error_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        genie_api_mock = Mock()
        genie_api_mock.get_message.side_effect = Exception("Message not found")

        client = DatabricksGenieClient(config)
        client.genie_api = genie_api_mock
        client.space_id = "space_123"
        client.conversation_id = "conv_456"

        # When & Then
        with pytest.raises(Exception, match="Message not found"):
            client.get_query_status("invalid_query_id")


class TestDatabricksGenieClientTableOperations:
    def test_list_tables_returns_table_names_from_specified_catalog_and_schema(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com",
            access_token="test_token",
            catalog="test_catalog",
            schema="test_schema",
        )

        table_mock_1 = Mock()
        table_mock_1.name = "customers"
        table_mock_2 = Mock()
        table_mock_2.name = "orders"
        table_mock_3 = Mock()
        table_mock_3.name = "products"

        tables_mock = Mock()
        tables_mock.list.return_value = [table_mock_1, table_mock_2, table_mock_3]

        workspace_client_mock = Mock()
        workspace_client_mock.tables = tables_mock

        client = DatabricksGenieClient(config)
        client.client = workspace_client_mock

        # When
        tables = client.list_tables()

        # Then
        assert tables == ["customers", "orders", "products"]
        tables_mock.list.assert_called_once_with(
            catalog_name="test_catalog", schema_name="test_schema"
        )

    def test_list_tables_uses_provided_catalog_and_schema_when_specified(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com",
            access_token="test_token",
            catalog="default_catalog",
            schema="default_schema",
        )

        table_mock = Mock()
        table_mock.name = "custom_table"

        tables_mock = Mock()
        tables_mock.list.return_value = [table_mock]

        workspace_client_mock = Mock()
        workspace_client_mock.tables = tables_mock

        client = DatabricksGenieClient(config)
        client.client = workspace_client_mock

        # When
        tables = client.list_tables(catalog="custom_catalog", schema="custom_schema")

        # Then
        assert tables == ["custom_table"]
        tables_mock.list.assert_called_once_with(
            catalog_name="custom_catalog", schema_name="custom_schema"
        )

    def test_list_tables_handles_api_error_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        tables_mock = Mock()
        tables_mock.list.side_effect = Exception("Cannot access catalog")

        workspace_client_mock = Mock()
        workspace_client_mock.tables = tables_mock

        client = DatabricksGenieClient(config)
        client.client = workspace_client_mock

        # When & Then
        with pytest.raises(Exception, match="Cannot access catalog"):
            client.list_tables()

    def test_get_available_tables_returns_formatted_table_information(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com",
            access_token="test_token",
            catalog="prod_catalog",
            schema="sales_schema",
        )

        table_mock_1 = Mock()
        table_mock_1.name = "customer_orders"
        table_mock_2 = Mock()
        table_mock_2.name = "product_catalog"

        tables_mock = Mock()
        tables_mock.list.return_value = [table_mock_1, table_mock_2]

        workspace_client_mock = Mock()
        workspace_client_mock.tables = tables_mock

        client = DatabricksGenieClient(config)
        client.client = workspace_client_mock

        # When
        result = client.get_available_tables()

        # Then
        expected_tables = [
            {
                "schema_name": "sales_schema",
                "name": "customer_orders",
                "display_name": "Customer Orders",
                "full_name": "prod_catalog.sales_schema.customer_orders",
            },
            {
                "schema_name": "sales_schema",
                "name": "product_catalog",
                "display_name": "Product Catalog",
                "full_name": "prod_catalog.sales_schema.product_catalog",
            },
        ]

        assert result["tables"] == expected_tables

    def test_get_available_tables_uses_provided_catalog_and_schema_when_specified(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com",
            access_token="test_token",
            catalog="default_catalog",
            schema="default_schema",
        )

        table_mock = Mock()
        table_mock.name = "special_table"

        tables_mock = Mock()
        tables_mock.list.return_value = [table_mock]

        workspace_client_mock = Mock()
        workspace_client_mock.tables = tables_mock

        client = DatabricksGenieClient(config)
        client.client = workspace_client_mock

        # When
        result = client.get_available_tables(
            catalog="special_catalog", schema="special_schema"
        )

        # Then
        expected_table = {
            "schema_name": "special_schema",
            "name": "special_table",
            "display_name": "Special Table",
            "full_name": "special_catalog.special_schema.special_table",
        }

        assert result["tables"] == [expected_table]

    def test_get_available_tables_handles_api_error_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        tables_mock = Mock()
        tables_mock.list.side_effect = Exception("Access denied to schema")

        workspace_client_mock = Mock()
        workspace_client_mock.tables = tables_mock

        client = DatabricksGenieClient(config)
        client.client = workspace_client_mock

        # When & Then
        with pytest.raises(Exception, match="Access denied to schema"):
            client.get_available_tables()


class TestDatabricksGenieClientResponseFormatting:
    def test_format_genie_response_formats_simple_explanation_response(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        client = DatabricksGenieClient(config)

        response = {
            "query": "Show me customer data",
            "explanation": "This query returns customer information from the database",
            "sql_query": "",
            "result": {"data": [], "summary": "No data returned"},
        }

        # When
        formatted = client.format_genie_response(response)

        # Then
        assert "This query returns customer information from the database" in formatted

    def test_format_genie_response_formats_response_with_sql_query(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        client = DatabricksGenieClient(config)

        response = {
            "query": "Show me top customers",
            "explanation": "Here are the top customers by sales",
            "sql_query": "SELECT customer_name, total_sales FROM customers ORDER BY total_sales DESC LIMIT 10",
            "result": {"data": [], "summary": "10 rows returned"},
        }

        # When
        formatted = client.format_genie_response(response)

        # Then
        assert "**SQL Query:**" in formatted
        assert "```sql" in formatted
        assert "SELECT customer_name, total_sales FROM customers" in formatted

    def test_format_genie_response_formats_response_with_data_results(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        client = DatabricksGenieClient(config)

        response = {
            "query": "Show me sales data",
            "explanation": "Sales data from last quarter",
            "sql_query": "SELECT * FROM sales",
            "result": {
                "data": [["Customer A", 1000], ["Customer B", 2000]],
                "columns": ["customer_name", "total_sales"],
                "summary": "2 rows returned",
            },
        }

        # When
        formatted = client.format_genie_response(response)

        # Then
        assert "**Results:**" in formatted
        assert "2 rows returned" in formatted
        assert "Customer A" in formatted
        assert "Customer B" in formatted

    def test_format_genie_response_handles_empty_response_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        client = DatabricksGenieClient(config)

        response = {
            "query": "",
            "explanation": "",
            "sql_query": "",
            "result": {"data": [], "summary": ""},
        }

        # When
        formatted = client.format_genie_response(response)

        # Then
        assert formatted == ""

    def test_format_genie_response_handles_formatting_error_gracefully(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        client = DatabricksGenieClient(config)

        # Create a mock object that will raise an exception when accessed
        class ProblematicMock:
            def __getitem__(self, key):
                raise Exception("Simulated formatting error")

            def get(self, key, default=None):
                raise Exception("Simulated formatting error")

        response = ProblematicMock()

        # When
        formatted = client.format_genie_response(response)

        # Then
        assert "Error formatting response" in formatted

    def test_format_genie_response_escapes_markdown_characters_properly(self):
        # Given
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com", access_token="test_token"
        )

        client = DatabricksGenieClient(config)

        response = {
            "query": "Show data with special chars",
            "explanation": "Results contain *asterisks* and _underscores_ and [brackets]",
            "sql_query": "",
            "result": {"data": [], "summary": ""},
        }

        # When
        formatted = client.format_genie_response(response)

        # Then
        assert "\\*asterisks\\*" in formatted
        assert "\\_underscores\\_" in formatted
        assert "\\[brackets\\]" in formatted
