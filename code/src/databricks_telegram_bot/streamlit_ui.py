import streamlit as st
import os
import sys
from typing import Dict, Any
from loguru import logger
import pandas as pd

from databricks_telegram_bot.config import DatabricksConfig
from databricks_telegram_bot.databricks_client import DatabricksGenieClient


class StreamlitGenieChat:
    """Streamlit UI wrapper for Databricks Genie Chat"""
    
    def __init__(self):
        self.config = None
        self.client = None
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging for the Streamlit app"""
        logger.remove()
        logger.add(
            sys.stderr,
            level="INFO",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
    
    def initialize_client(self) -> bool:
        """Initialize the Databricks client with configuration"""
        try:
            # Load configuration from environment variables
            self.config = DatabricksConfig(
                workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL", ""),
                access_token=os.getenv("DATABRICKS_ACCESS_TOKEN", ""),
                catalog=os.getenv("DATABRICKS_CATALOG", "hive_metastore"),
                schema=os.getenv("DATABRICKS_SCHEMA", "default")
            )
            
            # Validate required configuration
            if not self.config.workspace_url:
                st.error("❌ DATABRICKS_WORKSPACE_URL is not set")
                return False
            
            if not self.config.access_token:
                st.error("❌ DATABRICKS_ACCESS_TOKEN is not set")
                return False
            
            # Initialize the client
            self.client = DatabricksGenieClient(self.config)
            logger.info("Successfully initialized Databricks Genie client")
            return True
            
        except Exception as e:
            st.error(f"❌ Failed to initialize Databricks client: {str(e)}")
            logger.error(f"Failed to initialize client: {e}")
            return False
    
    def display_config_info(self):
        """Display current configuration in sidebar"""
        st.sidebar.header("📊 Configuration")
        
        if self.config:
            workspace_url = self.config.workspace_url
            # Mask the URL for security
            masked_url = workspace_url[:20] + "..." + workspace_url[-20:] if len(workspace_url) > 40 else workspace_url
            
            st.sidebar.success("✅ Configuration Loaded")
            st.sidebar.info(f"**Workspace:** {masked_url}")
            st.sidebar.info(f"**Catalog:** {self.config.catalog}")
            st.sidebar.info(f"**Schema:** {self.config.schema}")
        else:
            st.sidebar.error("❌ Configuration not loaded")
    
    def display_sample_queries(self):
        """Display sample queries in sidebar"""
        st.sidebar.header("💡 Sample Queries")
        
        sample_queries = [
            "Show me the top 10 customers by revenue",
            "What were the total sales last month?",
            "List all products with their categories",
            "Show me the revenue trend by month",
            "Which products have the highest profit margin?",
            "Show me customers from Madrid",
            "What are the total sales by product category?"
        ]
        
        for query in sample_queries:
            if st.sidebar.button(f"💬 {query}", key=f"sample_{hash(query)}"):
                # Set the selected query in session state
                st.session_state.selected_sample_query = query
                st.rerun()
    
    def format_response_for_streamlit(self, response: Dict[str, Any]) -> None:
        """Format and display the Genie response in Streamlit"""
        try:
            # Display explanation
            explanation = response.get("explanation", "")
            if explanation and explanation.strip():
                st.markdown("### 🤖 Genie Response")
                st.markdown(explanation)
            
            # Display SQL query
            sql_query = response.get("sql_query", "")
            if sql_query and sql_query.strip():
                st.markdown("### 📝 Generated SQL")
                st.code(sql_query, language="sql")
            
            # Display results
            result = response.get("result", {})
            data = result.get("data", [])
            
            if data:
                st.markdown("### 📊 Results")
                
                # Convert data to DataFrame for better display
                columns = result.get("columns", [])
                if columns and data and isinstance(data[0], list):
                    # Array format - convert to DataFrame
                    df = pd.DataFrame(data, columns=columns)
                    
                    # Display metrics if appropriate
                    if len(df) <= 100:
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.warning(f"⚠️ Large result set ({len(df)} rows). Showing first 100 rows.")
                        st.dataframe(df.head(100), use_container_width=True)
                    
                    # Display summary statistics for numeric columns
                    numeric_columns = df.select_dtypes(include=['number']).columns
                    if len(numeric_columns) > 0:
                        st.markdown("#### 📈 Summary Statistics")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Total Rows", len(df))
                        
                        if len(numeric_columns) > 0:
                            with col2:
                                first_numeric = numeric_columns[0]
                                total_value = df[first_numeric].sum()
                                st.metric(f"Total {first_numeric}", f"{total_value:,.2f}")
                            
                            with col3:
                                avg_value = df[first_numeric].mean()
                                st.metric(f"Avg {first_numeric}", f"{avg_value:,.2f}")
                elif isinstance(data[0], dict):
                    # Dictionary format
                    df = pd.DataFrame(data)
                    st.dataframe(df, use_container_width=True)
                else:
                    # Unknown format, display as raw data
                    st.json(data)
                
                # Display summary
                summary = result.get("summary", "")
                if summary:
                    st.info(f"ℹ️ {summary}")
            else:
                st.warning("⚠️ No data returned from the query")
        
        except Exception as e:
            st.error(f"❌ Error displaying results: {str(e)}")
            logger.error(f"Error formatting response for Streamlit: {e}")
    
    def handle_query(self, query: str):
        """Handle a user query and display results"""
        if not query.strip():
            st.warning("⚠️ Please enter a question")
            return
        
        if not self.client:
            st.error("❌ Databricks client not initialized")
            return
        
        try:
            with st.spinner("🤔 Thinking... (this may take a moment)"):
                # Send query to Genie
                response = self.client.query_genie(query)
                
                # Store in session state
                if "chat_history" not in st.session_state:
                    st.session_state.chat_history = []
                
                st.session_state.chat_history.append({
                    "query": query,
                    "response": response,
                    "timestamp": pd.Timestamp.now()
                })
                
                # Display the response
                self.format_response_for_streamlit(response)
        
        except Exception as e:
            st.error(f"❌ Error processing query: {str(e)}")
            logger.error(f"Error processing query: {e}")
    
    def display_chat_history(self):
        """Display chat history with last 2 chats expanded, others collapsed"""
        if "chat_history" not in st.session_state or not st.session_state.chat_history:
            return
        
        st.markdown("---")
        st.markdown("### 📝 Chat History")
        
        # Get the last 10 chats for display
        recent_chats = list(reversed(st.session_state.chat_history[-10:]))
        
        for i, chat in enumerate(recent_chats):
            # Expand the first 2 chats (most recent), collapse the rest
            is_expanded = i < 2
            
            # Create a shorter title for the expander
            chat_title = f"🕐 {chat['timestamp'].strftime('%H:%M:%S')} - {chat['query'][:50]}{'...' if len(chat['query']) > 50 else ''}"
            
            with st.expander(chat_title, expanded=is_expanded):
                st.markdown(f"**Question:** {chat['query']}")
                
                explanation = chat['response'].get('explanation', '')
                if explanation:
                    st.markdown(f"**Response:** {explanation}")
                
                sql_query = chat['response'].get('sql_query', '')
                if sql_query:
                    st.code(sql_query, language="sql")
                
                # Show result summary if available
                result = chat['response'].get('result', {})
                summary = result.get('summary', '')
                if summary:
                    st.info(f"ℹ️ {summary}")
    
    def run(self):
        """Main Streamlit app"""
        # Configure page
        st.set_page_config(
            page_title="Appinnova: UCM Big Data Master",
            page_icon="🎓",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Header
        st.title("🎓 Appinnova: UCM Big Data Master")
        st.markdown("Ask questions about your data in natural language!")
        
        # Initialize client
        if not self.initialize_client():
            st.stop()
        
        # Display configuration info
        self.display_config_info()
        
        # Display sample queries
        self.display_sample_queries()
        
        # Main chat interface
        st.markdown("### 💬 Ask a Question")
        
        # Initialize session state for query input
        if "selected_sample_query" not in st.session_state:
            st.session_state.selected_sample_query = ""
        if "last_submitted_query" not in st.session_state:
            st.session_state.last_submitted_query = ""
        
        # Check if a sample query was selected
        default_value = ""
        if st.session_state.selected_sample_query:
            default_value = st.session_state.selected_sample_query
            # Clear it so it doesn't persist
            st.session_state.selected_sample_query = ""
        
        # Query input
        col1, col2 = st.columns([4, 1])
        
        with col1:
            query = st.text_input(
                "Enter your question:",
                value=default_value,
                placeholder="e.g., Show me the top 10 customers by revenue",
                key="query_text_input"
            )
        
        with col2:
            submit_button = st.button("🚀 Ask", type="primary")
        
        # Handle query submission (both Enter key and button click)
        if (submit_button or (query and query != st.session_state.last_submitted_query)) and query.strip():
            st.session_state.last_submitted_query = query
            self.handle_query(query)
        
        # Display chat history
        self.display_chat_history()
        
        # Footer
        st.markdown("---")
        st.markdown("*UCM Big Data Master - Powered by Databricks Genie and Streamlit*")


def main():
    """Main entry point"""
    app = StreamlitGenieChat()
    app.run()


if __name__ == "__main__":
    main()
