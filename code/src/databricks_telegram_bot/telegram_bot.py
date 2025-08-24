"""
Telegram bot module for handling user interactions
"""

import telebot
from typing import Optional, Dict, Any
from loguru import logger
from .config import TelegramConfig
from .databricks_client import DatabricksGenieClient


class DatabricksTelegramBot:
    """Telegram bot for Databricks Genie queries"""
    
    def __init__(self, config: TelegramConfig, databricks_client: DatabricksGenieClient, bot: Optional[telebot.TeleBot] = None):
        self.config = config
        self.databricks_client = databricks_client
        self.bot = bot if bot is not None else telebot.TeleBot(config.bot_token)
        
        # Register handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register all bot handlers"""
        # Command handlers
        @self.bot.message_handler(commands=['start'])
        def start_command(message):
            self.start_command(message)
        
        @self.bot.message_handler(commands=['help'])
        def help_command(message):
            self.help_command(message)
        
        @self.bot.message_handler(commands=['tables'])
        def tables_command(message):
            self.tables_command(message)
        
        @self.bot.message_handler(commands=['status'])
        def status_command(message):
            self.status_command(message)
        
        # Message handler for natural language queries
        @self.bot.message_handler(func=lambda message: True)
        def handle_message(message):
            self.handle_message(message)
    
    def start_command(self, message):
        """Handle /start command"""
        user_id = str(message.from_user.id)
        
        if not self._is_user_authorized(user_id):
            self.bot.reply_to(message, 
                "❌ Sorry, you are not authorized to use this bot. "
                "Please contact the administrator.")
            return
        
        welcome_message = (
            "🤖 **Welcome to Databricks Genie Bot!**\n\n"
            "I can help you query your data using natural language. "
            "Just ask me questions like:\n\n"
            "• \"Show me the top 10 customers by revenue\"\n"
            "• \"What's the total sales for this month?\"\n"
            "• \"Find invoices with amounts greater than $1000\"\n\n"
            "Use /help for more commands and examples."
        )
        
        self.bot.reply_to(message, welcome_message, parse_mode='Markdown')
    
    def help_command(self, message):
        """Handle /help command"""
        user_id = str(message.from_user.id)
        
        if not self._is_user_authorized(user_id):
            return
        
        help_message = (
            "📚 **Available Commands:**\n\n"
            "/start - Start the bot and see welcome message\n"
            "/help - Show this help message\n"
            "/tables - List available tables in your catalog\n"
            "/status - Check bot status\n\n"
            "💡 **How to ask questions:**\n\n"
            "Just type your question in natural language. For example:\n"
            "• \"What are the top 5 products by sales?\"\n"
            "• \"Show me customer data from last month\"\n"
            "• \"Calculate total revenue by region\"\n"
            "• \"Find all invoices above $500\"\n\n"
            "🔍 **Tips:**\n"
            "• Be specific in your questions\n"
            "• Mention time periods if relevant\n"
            "• Use filters like 'above', 'below', 'between'\n"
            "• Ask for summaries or detailed data"
        )
        
        self.bot.reply_to(message, help_message, parse_mode='Markdown')
    
    def tables_command(self, message):
        """Handle /tables command to show available tables"""
        user_id = str(message.from_user.id)
        
        if not self._is_user_authorized(user_id):
            return
        
        try:
            processing_message = self.bot.reply_to(message, "🔍 Fetching available tables...")
            
            tables_response = self.databricks_client.get_available_tables()
            tables = tables_response.get("tables", [])
            
            if not tables:
                self.bot.edit_message_text("No tables found in the current catalog/schema.", 
                                         chat_id=message.chat.id, 
                                         message_id=processing_message.message_id)
                return
            
            # Group tables by schema
            schemas = {}
            for table in tables:
                schema_name = table.get("schema_name", "unknown")
                if schema_name not in schemas:
                    schemas[schema_name] = []
                schemas[schema_name].append({
                    "name": table.get("name", "unknown"),
                    "display_name": table.get("display_name", table.get("name", "unknown"))
                })
            
            # Format response with better formatting
            response = "📋 **Available Tables:**\n\n"
            for schema_name, table_list in schemas.items():
                response += f"**Schema: {schema_name.title()}**\n"
                # Sort tables by display name for better readability
                sorted_tables = sorted(table_list, key=lambda x: x["display_name"])
                for table_info in sorted_tables:
                    response += f"• **{table_info['display_name']}** (`{table_info['name']}`)\n"
                response += "\n"
            
            # Check if response is too long
            if len(response) > 4096:
                # Split into multiple messages
                chunks = [response[i:i+4096] for i in range(0, len(response), 4096)]
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        self.bot.edit_message_text(chunk, 
                                                 chat_id=message.chat.id, 
                                                 message_id=processing_message.message_id,
                                                 parse_mode='Markdown')
                    else:
                        self.bot.send_message(message.chat.id, chunk, parse_mode='Markdown')
            else:
                self.bot.edit_message_text(response, 
                                         chat_id=message.chat.id, 
                                         message_id=processing_message.message_id,
                                         parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error fetching tables: {e}")
            error_message = (
                f"❌ **Error fetching tables:**\n\n"
                f"```\n{str(e)}\n```\n\n"
                "Please try again later or contact support if the issue persists."
            )
            try:
                self.bot.edit_message_text(error_message, 
                                         chat_id=message.chat.id, 
                                         message_id=processing_message.message_id,
                                         parse_mode='Markdown')
            except:
                self.bot.reply_to(message, error_message, parse_mode='Markdown')
    
    def status_command(self, message):
        """Handle /status command"""
        user_id = str(message.from_user.id)
        
        if not self._is_user_authorized(user_id):
            return
        
        try:
            # Test Databricks connection
            self.databricks_client.get_available_tables()
            databricks_status = "✅ Connected"
        except Exception as e:
            logger.warning(f"Databricks connection test failed: {e}")
            databricks_status = "❌ Disconnected"
        
        status_message = (
            "✅ **Bot Status:**\n\n"
            "🤖 Bot: Online\n"
            f"🔗 Databricks: {databricks_status}\n"
            "📊 Genie: Available\n\n"
            "Ready to answer your questions!"
        )
        
        self.bot.reply_to(message, status_message, parse_mode='Markdown')
    
    def handle_message(self, message):
        """Handle natural language queries"""
        user_id = str(message.from_user.id)
        
        if not self._is_user_authorized(user_id):
            return
        
        question = message.text.strip()
        
        if not question:
            self.bot.reply_to(message, "Please provide a question to query your data.")
            return
        
        # Send typing indicator
        self.bot.send_chat_action(message.chat.id, 'typing')
        
        try:
            # Send initial response
            processing_message = self.bot.reply_to(message,
                f"🔍 Processing your question: \"{question}\"\n\n"
                "This may take a few moments...")
            
            # Query Databricks Genie
            response = self.databricks_client.query_genie(question)
            
            # Format the response
            formatted_response = self.databricks_client.format_genie_response(response)
            
            # Check if response is too long for Telegram
            if len(formatted_response) > 4096:
                # Split into multiple messages
                chunks = [formatted_response[i:i+4096] for i in range(0, len(formatted_response), 4096)]
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        self.bot.edit_message_text(chunk, 
                                                 chat_id=message.chat.id, 
                                                 message_id=processing_message.message_id,
                                                 parse_mode='Markdown')
                    else:
                        self.bot.send_message(message.chat.id, chunk, parse_mode='Markdown')
            else:
                self.bot.edit_message_text(formatted_response, 
                                         chat_id=message.chat.id, 
                                         message_id=processing_message.message_id,
                                         parse_mode='Markdown')
            
            logger.info(f"Successfully processed query for user {user_id}: {question}")
            
        except Exception as e:
            logger.error(f"Error processing query for user {user_id}: {e}")
            error_message = (
                f"❌ **Error processing your question:**\n\n"
                f"```\n{str(e)}\n```\n\n"
                "Please try rephrasing your question or contact support if the issue persists."
            )
            
            try:
                self.bot.edit_message_text(error_message, 
                                         chat_id=message.chat.id, 
                                         message_id=processing_message.message_id,
                                         parse_mode='Markdown')
            except:
                self.bot.reply_to(message, error_message, parse_mode='Markdown')
    
    def _is_user_authorized(self, user_id: str) -> bool:
        """Check if user is authorized to use the bot"""
        if not self.config.allowed_users:
            return False
        return user_id in self.config.allowed_users
    
    def start(self):
        """Start the bot"""
        logger.info("Starting Databricks Telegram Bot...")
        try:
            logger.info("Bot is ready! Press Ctrl+C to stop.")
            # Use polling with proper interrupt handling
            self.bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, stopping bot...")
            self.stop()
        except Exception as e:
            error_msg = str(e)
            if "409" in error_msg and "Conflict" in error_msg:
                logger.error("Bot conflict detected! Another instance is already running.")
                logger.error("Please ensure only one bot instance is running at a time.")
                logger.error("If this is incorrect, try deleting the bot.pid file and restarting.")
            else:
                logger.error(f"Failed to start Telegram bot: {e}")
            raise
    
    def stop(self):
        """Stop the bot"""
        logger.info("Stopping Databricks Telegram Bot...")
        try:
            # Stop the polling
            self.bot.stop_polling()
            logger.info("Bot polling stopped successfully")
            
            # Force stop any remaining threads
            import threading
            for thread in threading.enumerate():
                if thread.name.startswith('Thread-') and thread.is_alive():
                    logger.info(f"Stopping thread: {thread.name}")
                    thread.join(timeout=2)
                    
        except Exception as e:
            logger.error(f"Error stopping Telegram bot: {e}")
            raise

