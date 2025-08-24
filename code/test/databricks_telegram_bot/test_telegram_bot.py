import pytest
from unittest.mock import Mock
from src.databricks_telegram_bot.telegram_bot import DatabricksTelegramBot
from src.databricks_telegram_bot.config import TelegramConfig


class TestDatabricksTelegramBotUserAuthorization:
    def test_user_authorization_when_user_is_in_allowed_list(self):
        # Given
        allowed_users = ["123456789", "987654321"]
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=allowed_users)
        databricks_client = Mock()
        mock_telegram_bot = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=mock_telegram_bot)
        
        # When
        is_authorized = bot._is_user_authorized("123456789")
        
        # Then
        assert is_authorized is True
    
    def test_user_authorization_when_user_is_not_in_allowed_list(self):
        # Given
        allowed_users = ["123456789", "987654321"]
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=allowed_users)
        databricks_client = Mock()
        mock_telegram_bot = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=mock_telegram_bot)
        
        # When
        is_authorized = bot._is_user_authorized("555555555")
        
        # Then
        assert is_authorized is False
    
    def test_user_authorization_when_allowed_users_list_is_empty(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=[])
        databricks_client = Mock()
        mock_telegram_bot = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=mock_telegram_bot)
        
        # When
        is_authorized = bot._is_user_authorized("123456789")
        
        # Then
        assert is_authorized is False
    
    def test_user_authorization_when_allowed_users_is_none(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=None)
        databricks_client = Mock()
        mock_telegram_bot = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=mock_telegram_bot)
        
        # When
        is_authorized = bot._is_user_authorized("123456789")
        
        # Then
        assert is_authorized is False


class TestDatabricksTelegramBotStartCommand:
    def test_start_command_sends_welcome_message_for_authorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/start")
        
        # When
        bot.start_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_called_once()
        call_args = telegram_bot_mock.reply_to.call_args
        response_text = call_args[0][1]
        
        assert "Welcome to Databricks Genie Bot" in response_text
        assert "natural language" in response_text
        assert "/help" in response_text
    
    def test_start_command_sends_unauthorized_message_for_unauthorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["987654321"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/start")
        
        # When
        bot.start_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_called_once()
        call_args = telegram_bot_mock.reply_to.call_args
        response_text = call_args[0][1]
        
        assert "not authorized" in response_text
        assert "administrator" in response_text
    
    def _create_message_mock(self, user_id: int, text: str):
        message = Mock()
        message.from_user.id = user_id
        message.chat.id = user_id
        message.text = text
        return message


class TestDatabricksTelegramBotHelpCommand:
    def test_help_command_sends_help_message_for_authorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/help")
        
        # When
        bot.help_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_called_once()
        call_args = telegram_bot_mock.reply_to.call_args
        response_text = call_args[0][1]
        
        assert "Available Commands" in response_text
        assert "/start" in response_text
        assert "/tables" in response_text
        assert "/status" in response_text
        assert "natural language" in response_text
    
    def test_help_command_does_not_respond_for_unauthorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["987654321"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/help")
        
        # When
        bot.help_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_not_called()
    
    def _create_message_mock(self, user_id: int, text: str):
        message = Mock()
        message.from_user.id = user_id
        message.chat.id = user_id
        message.text = text
        return message


class TestDatabricksTelegramBotTablesCommand:
    def test_tables_command_displays_tables_grouped_by_schema_for_authorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        
        mock_tables_response = {
            "tables": [
                {
                    "schema_name": "sales",
                    "name": "customers",
                    "display_name": "Customer Data"
                },
                {
                    "schema_name": "sales", 
                    "name": "orders",
                    "display_name": "Order Records"
                },
                {
                    "schema_name": "finance",
                    "name": "invoices", 
                    "display_name": "Invoice Details"
                }
            ]
        }
        
        databricks_client = Mock()
        databricks_client.get_available_tables.return_value = mock_tables_response
        
        telegram_bot_mock = Mock()
        processing_message_mock = Mock()
        processing_message_mock.message_id = 12345
        telegram_bot_mock.reply_to.return_value = processing_message_mock
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/tables")
        
        # When
        bot.tables_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_called_once_with(message, "🔍 Fetching available tables...")
        telegram_bot_mock.edit_message_text.assert_called_once()
        
        edit_call_args = telegram_bot_mock.edit_message_text.call_args
        response_text = edit_call_args[0][0]
        
        assert "Available Tables" in response_text
        assert "Schema: Sales" in response_text
        assert "Schema: Finance" in response_text
        assert "Customer Data" in response_text
        assert "Order Records" in response_text
        assert "Invoice Details" in response_text
    
    def test_tables_command_shows_no_tables_message_when_no_tables_available(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        
        databricks_client = Mock()
        databricks_client.get_available_tables.return_value = {"tables": []}
        
        telegram_bot_mock = Mock()
        processing_message_mock = Mock()
        processing_message_mock.message_id = 12345
        telegram_bot_mock.reply_to.return_value = processing_message_mock
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/tables")
        
        # When
        bot.tables_command(message)
        
        # Then
        telegram_bot_mock.edit_message_text.assert_called_once()
        edit_call_args = telegram_bot_mock.edit_message_text.call_args
        response_text = edit_call_args[0][0]
        
        assert "No tables found" in response_text
    
    def test_tables_command_handles_databricks_client_error_gracefully(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        
        databricks_client = Mock()
        databricks_client.get_available_tables.side_effect = Exception("Connection timeout")
        
        telegram_bot_mock = Mock()
        processing_message_mock = Mock()
        processing_message_mock.message_id = 12345
        telegram_bot_mock.reply_to.return_value = processing_message_mock
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/tables")
        
        # When
        bot.tables_command(message)
        
        # Then
        telegram_bot_mock.edit_message_text.assert_called_once()
        edit_call_args = telegram_bot_mock.edit_message_text.call_args
        response_text = edit_call_args[0][0]
        
        assert "Error fetching tables" in response_text
        assert "Connection timeout" in response_text
    
    def test_tables_command_does_not_respond_for_unauthorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["987654321"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/tables")
        
        # When
        bot.tables_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_not_called()
        telegram_bot_mock.edit_message_text.assert_not_called()
    
    def _create_message_mock(self, user_id: int, text: str):
        """Helper method to create a message mock with required properties"""
        message = Mock()
        message.from_user.id = user_id
        message.chat.id = user_id
        message.text = text
        return message


class TestDatabricksTelegramBotStatusCommand:
    def test_status_command_shows_connected_status_when_databricks_is_available(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        
        databricks_client = Mock()
        databricks_client.get_available_tables.return_value = {"tables": []}
        
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/status")
        
        # When
        bot.status_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_called_once()
        call_args = telegram_bot_mock.reply_to.call_args
        response_text = call_args[0][1]
        
        assert "Bot Status" in response_text
        assert "Bot: Online" in response_text
        assert "Databricks: ✅ Connected" in response_text
        assert "Genie: Available" in response_text
    
    def test_status_command_shows_disconnected_status_when_databricks_is_unavailable(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        
        databricks_client = Mock()
        databricks_client.get_available_tables.side_effect = Exception("Connection failed")
        
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/status")
        
        # When
        bot.status_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_called_once()
        call_args = telegram_bot_mock.reply_to.call_args
        response_text = call_args[0][1]
        
        assert "Bot Status" in response_text
        assert "Bot: Online" in response_text
        assert "Databricks: ❌ Disconnected" in response_text
        assert "Genie: Available" in response_text
    
    def test_status_command_does_not_respond_for_unauthorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["987654321"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="/status")
        
        # When
        bot.status_command(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_not_called()
    
    def _create_message_mock(self, user_id: int, text: str):
        message = Mock()
        message.from_user.id = user_id
        message.chat.id = user_id
        message.text = text
        return message


class TestDatabricksTelegramBotHandleMessage:
    def test_handle_message_processes_valid_question_for_authorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        
        mock_genie_response = {
            "query": "Show me top customers",
            "sql_query": "SELECT * FROM customers LIMIT 10",
            "result": {"data": [["Customer A"], ["Customer B"]], "summary": "2 rows"},
            "explanation": "This query returns the top customers from the database"
        }
        
        databricks_client = Mock()
        databricks_client.query_genie.return_value = mock_genie_response
        databricks_client.format_genie_response.return_value = "Formatted response with SQL and results"
        
        telegram_bot_mock = Mock()
        processing_message_mock = Mock()
        processing_message_mock.message_id = 12345
        telegram_bot_mock.reply_to.return_value = processing_message_mock
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="Show me top customers")
        
        # When
        bot.handle_message(message)
        
        # Then
        telegram_bot_mock.send_chat_action.assert_called_once_with(123456789, 'typing')
        databricks_client.query_genie.assert_called_once_with("Show me top customers")
        databricks_client.format_genie_response.assert_called_once_with(mock_genie_response)
        telegram_bot_mock.edit_message_text.assert_called_once()
        
        edit_call_args = telegram_bot_mock.edit_message_text.call_args
        response_text = edit_call_args[0][0]
        assert response_text == "Formatted response with SQL and results"
    
    def test_handle_message_rejects_empty_question(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="   ")
        
        # When
        bot.handle_message(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_called_once()
        call_args = telegram_bot_mock.reply_to.call_args
        response_text = call_args[0][1]
        
        assert "Please provide a question" in response_text
        databricks_client.query_genie.assert_not_called()
    
    def test_handle_message_handles_databricks_client_error_gracefully(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        
        databricks_client = Mock()
        databricks_client.query_genie.side_effect = Exception("Query execution failed")
        
        telegram_bot_mock = Mock()
        processing_message_mock = Mock()
        processing_message_mock.message_id = 12345
        telegram_bot_mock.reply_to.return_value = processing_message_mock
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="Show me data")
        
        # When
        bot.handle_message(message)
        
        # Then
        telegram_bot_mock.edit_message_text.assert_called_once()
        edit_call_args = telegram_bot_mock.edit_message_text.call_args
        response_text = edit_call_args[0][0]
        
        assert "Error processing your question" in response_text
        assert "Query execution failed" in response_text
    
    def test_handle_message_splits_long_response_into_multiple_messages(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        
        # Create a response longer than Telegram's 4096 character limit
        long_response = "A" * 5000
        
        mock_genie_response = {"query": "test", "explanation": "test"}
        
        databricks_client = Mock()
        databricks_client.query_genie.return_value = mock_genie_response
        databricks_client.format_genie_response.return_value = long_response
        
        telegram_bot_mock = Mock()
        processing_message_mock = Mock()
        processing_message_mock.message_id = 12345
        telegram_bot_mock.reply_to.return_value = processing_message_mock
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="Show me lots of data")
        
        # When
        bot.handle_message(message)
        
        # Then
        telegram_bot_mock.edit_message_text.assert_called_once()
        telegram_bot_mock.send_message.assert_called()
        
        # Verify the first chunk was edited
        edit_call_args = telegram_bot_mock.edit_message_text.call_args
        first_chunk = edit_call_args[0][0]
        assert len(first_chunk) <= 4096
        
        # Verify additional messages were sent
        send_call_args = telegram_bot_mock.send_message.call_args
        assert send_call_args[0][0] == 123456789  # chat_id
    
    def test_handle_message_does_not_respond_for_unauthorized_user(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["987654321"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        message = self._create_message_mock(user_id=123456789, text="Show me data")
        
        # When
        bot.handle_message(message)
        
        # Then
        telegram_bot_mock.reply_to.assert_not_called()
        telegram_bot_mock.send_chat_action.assert_not_called()
        databricks_client.query_genie.assert_not_called()
    
    def _create_message_mock(self, user_id: int, text: str):
        """Helper method to create a message mock with required properties"""
        message = Mock()
        message.from_user.id = user_id
        message.chat.id = user_id
        message.text = text
        return message


class TestDatabricksTelegramBotInitialization:
    def test_bot_initializes_with_valid_config_and_databricks_client(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        # When
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        # Then
        assert bot.config == config
        assert bot.databricks_client == databricks_client
        assert bot.bot == telegram_bot_mock
    
    def test_bot_stores_configuration_properties_correctly(self):
        # Given
        expected_token = "654321:XYZ-ABC9876fedcba-lmn43O9v8u765zy22"
        expected_users = ["111111111", "222222222", "333333333"]
        config = TelegramConfig(bot_token=expected_token, allowed_users=expected_users)
        databricks_client = Mock()
        telegram_bot_mock = Mock()
        
        # When
        bot = DatabricksTelegramBot(config, databricks_client, bot=telegram_bot_mock)
        
        # Then
        assert bot.config.bot_token == expected_token
        assert bot.config.allowed_users == expected_users
    
    def test_bot_stores_databricks_client_reference_correctly(self):
        # Given
        config = TelegramConfig(bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", allowed_users=["123456789"])
        expected_databricks_client = Mock()
        expected_databricks_client.workspace_url = "https://test.databricks.com"
        telegram_bot_mock = Mock()
        
        # When
        bot = DatabricksTelegramBot(config, expected_databricks_client, bot=telegram_bot_mock)
        
        # Then
        assert bot.databricks_client == expected_databricks_client
        assert bot.databricks_client.workspace_url == "https://test.databricks.com"
