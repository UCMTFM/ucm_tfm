import signal
import sys
import threading
import os
import atexit
from pathlib import Path
from loguru import logger
from databricks_telegram_bot.config import load_config, validate_config
from databricks_telegram_bot.databricks_client import DatabricksGenieClient
from databricks_telegram_bot.telegram_bot import DatabricksTelegramBot


class DatabricksGenieBotApp: 
    def __init__(self):
        self.config = None
        self.databricks_client = None
        self.telegram_bot = None
        self.running = False
        self._shutdown_event = threading.Event()
        self.pid_file = Path("bot.pid")
    
    def check_single_instance(self):
        """Ensure only one instance of the bot is running"""
        if self.pid_file.exists():
            try:
                with open(self.pid_file, 'r') as f:
                    old_pid = int(f.read().strip())
                
                # Check if the process is still running
                try:
                    os.kill(old_pid, 0)  # This will raise an error if process doesn't exist
                    logger.error(f"Bot is already running with PID {old_pid}")
                    logger.error("If this is incorrect, delete the bot.pid file and try again")
                    sys.exit(1)
                except OSError:
                    # Process doesn't exist, remove stale PID file
                    logger.warning(f"Removing stale PID file (PID {old_pid} not running)")
                    self.pid_file.unlink()
            except (ValueError, IOError):
                # Invalid PID file, remove it
                logger.warning("Removing invalid PID file")
                self.pid_file.unlink()
        
        # Create PID file
        with open(self.pid_file, 'w') as f:
            f.write(str(os.getpid()))
        
        # Register cleanup function
        atexit.register(self._cleanup_pid_file)
    
    def _cleanup_pid_file(self):
        """Clean up PID file on exit"""
        try:
            if self.pid_file.exists():
                self.pid_file.unlink()
                logger.info("PID file cleaned up")
        except Exception as e:
            logger.warning(f"Could not clean up PID file: {e}")
    
    def setup_logging(self):
        """Setup logging configuration"""
        # Create logs directory if it doesn't exist
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        logger.remove()  # Remove default handler
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level=self.config.log_level
        )
        logger.add(
            logs_dir / "bot.log",
            rotation="10 MB",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG"
        )
    
    def load_configuration(self):
        """Load and validate configuration"""
        try:
            self.config = load_config()
            validate_config(self.config)
            logger.info("Configuration loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            sys.exit(1)
    
    def initialize_components(self):
        """Initialize all application components"""
        try:
            # Initialize Databricks client
            self.databricks_client = DatabricksGenieClient(self.config.databricks)
            logger.info("Databricks client initialized")
            
            # Initialize Telegram bot
            self.telegram_bot = DatabricksTelegramBot(self.config.telegram, self.databricks_client)
            logger.info("Telegram bot initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            sys.exit(1)
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.running = False
            self._shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def start(self):
        """Start the application"""
        logger.info("Starting Databricks Genie Telegram Bot application...")
        
        try:
            # Check for single instance
            self.check_single_instance()
            
            # Load configuration
            self.load_configuration()
            
            # Setup logging
            self.setup_logging()
            
            # Initialize components
            self.initialize_components()
            
            # Setup signal handlers
            self.setup_signal_handlers()
            
            # Start the bot
            self.running = True
            logger.info("Bot is ready! Press Ctrl+C to stop.")
            
            # Start bot in a separate thread to allow for graceful shutdown
            bot_thread = threading.Thread(target=self._run_bot)
            bot_thread.daemon = True
            bot_thread.start()
            
            # Wait for shutdown signal
            try:
                self._shutdown_event.wait()
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
            finally:
                self.running = False
                self.stop()
            
        except Exception as e:
            logger.error(f"Failed to start application: {e}")
            sys.exit(1)
    
    def _run_bot(self):
        """Run the bot in a separate thread"""
        try:
            self.telegram_bot.start()
        except KeyboardInterrupt:
            logger.info("Bot received keyboard interrupt")
        except Exception as e:
            logger.error(f"Bot error: {e}")
        finally:
            # Ensure we signal shutdown even if bot fails
            self._shutdown_event.set()
    
    def stop(self):
        """Stop the application"""
        logger.info("Stopping Databricks Genie Telegram Bot application...")
        
        try:
            if self.telegram_bot:
                self.telegram_bot.stop()
            
            # Wait a bit for the bot thread to finish
            import time
            time.sleep(1)
            
            logger.info("Application stopped successfully")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        finally:
            # Ensure PID file is cleaned up
            self._cleanup_pid_file()


def main():
    app = DatabricksGenieBotApp()
    
    try:
        app.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        app.stop()


if __name__ == "__main__":
    main()