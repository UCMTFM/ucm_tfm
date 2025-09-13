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
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        )
        logger.add(
            sys.stderr,
            level="INFO",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        )

    def initialize_client(self) -> bool:
        try:
            # Load configuration from environment variables
            self.config = DatabricksConfig(
                workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL", ""),
                access_token=os.getenv("DATABRICKS_ACCESS_TOKEN", ""),
                catalog=os.getenv("DATABRICKS_CATALOG", "hive_metastore"),
                schema=os.getenv("DATABRICKS_SCHEMA", "default"),
            )

            # Validate required configuration
            if not self.config.workspace_url:
                st.error("❌ DATABRICKS_WORKSPACE_URL no está configurado")
                return False

            if not self.config.access_token:
                st.error("❌ DATABRICKS_ACCESS_TOKEN no está configurado")
                return False

            # Initialize the client
            self.client = DatabricksGenieClient(self.config)
            logger.info("Successfully initialized Databricks Genie client")
            return True

        except Exception as e:
            st.error(f"❌ Error al inicializar el cliente de Databricks: {str(e)}")
            logger.error(f"Failed to initialize client: {e}")
            return False

    def display_config_info(self):
        st.sidebar.header("📊 Configuración")

        if self.config:
            workspace_url = self.config.workspace_url
            # Mask the URL for security
            masked_url = (
                workspace_url[:20] + "..." + workspace_url[-20:]
                if len(workspace_url) > 40
                else workspace_url
            )

            st.sidebar.success("✅ Configuración Cargada")
            st.sidebar.info(f"**Workspace:** {masked_url}")
            st.sidebar.info(f"**Catálogo:** {self.config.catalog}")
            st.sidebar.info(f"**Esquema:** {self.config.schema}")
        else:
            st.sidebar.error("❌ Configuración no cargada")

    def display_sample_queries(self):
        st.sidebar.header("💡 Consultas de Ejemplo")

        sample_queries = [
            "¿Cuáles son las 10 facturas con mayor valor total?",
            "¿Cuáles son los productos más vendidos por cantidad?",
            "Muéstrame las facturas que tienen saldo pendiente",
            "¿Cuáles son las facturas anuladas y sus motivos?",
            "¿Qué productos tienen el mayor valor unitario?",
        ]

        for query in sample_queries:
            if st.sidebar.button(f"💬 {query}", key=f"sample_{hash(query)}"):
                # Set the selected query in session state
                st.session_state.selected_sample_query = query
                st.rerun()

    def format_sql_query(self, sql_query: str) -> str:
        """Format SQL query with better presentation"""
        if not sql_query:
            return sql_query

        # Basic SQL formatting - add line breaks after common keywords
        formatted_sql = sql_query.replace(" FROM ", "\nFROM ")
        formatted_sql = formatted_sql.replace(" WHERE ", "\nWHERE ")
        formatted_sql = formatted_sql.replace(" GROUP BY ", "\nGROUP BY ")
        formatted_sql = formatted_sql.replace(" ORDER BY ", "\nORDER BY ")
        formatted_sql = formatted_sql.replace(" HAVING ", "\nHAVING ")
        formatted_sql = formatted_sql.replace(" LIMIT ", "\nLIMIT ")
        formatted_sql = formatted_sql.replace(" JOIN ", "\nJOIN ")
        formatted_sql = formatted_sql.replace(" LEFT JOIN ", "\nLEFT JOIN ")
        formatted_sql = formatted_sql.replace(" RIGHT JOIN ", "\nRIGHT JOIN ")
        formatted_sql = formatted_sql.replace(" INNER JOIN ", "\nINNER JOIN ")
        formatted_sql = formatted_sql.replace(" AND ", "\n  AND ")
        formatted_sql = formatted_sql.replace(" OR ", "\n  OR ")

        return formatted_sql

    def format_dataframe_for_display(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame columns for better display"""
        df_display = df.copy()

        # Format numeric columns to avoid scientific notation
        for col in df_display.columns:
            if df_display[col].dtype in ["float64", "int64", "float32", "int32"]:

                def format_number(x):
                    if pd.isna(x):
                        return ""

                    # Convert to float to handle both int and float
                    num = float(x)

                    # Check if the number is effectively an integer
                    is_integer = num == int(num)

                    # Handle very large numbers - format as clean numbers with commas
                    if abs(num) >= 1_000_000_000:  # Billions
                        return f"{num:,.0f}" if is_integer else f"{num:,.2f}"
                    elif abs(num) >= 1_000_000:  # Millions
                        return f"{num:,.0f}" if is_integer else f"{num:,.2f}"
                    elif abs(num) >= 1_000:  # Thousands
                        return f"{num:,.0f}" if is_integer else f"{num:,.2f}"
                    elif abs(num) >= 1:
                        return f"{num:.0f}" if is_integer else f"{num:.2f}"
                    else:
                        # Small decimals - always show decimals since they're meaningful
                        return f"{num:.4f}"

                # Apply the formatting function
                df_display[col] = df_display[col].apply(format_number)

        # Truncate long string columns for better display
        for col in df_display.columns:
            if (
                df_display[col].dtype == "object"
                and col not in df.select_dtypes(include=["number"]).columns
            ):
                df_display[col] = (
                    df_display[col]
                    .astype(str)
                    .apply(lambda x: x[:50] + "..." if len(x) > 50 else x)
                )

        return df_display

    def format_response_for_streamlit(self, response: Dict[str, Any]) -> None:
        """Format and display the Genie response in Streamlit with improved presentation"""
        try:
            # Display explanation with better formatting
            explanation = response.get("explanation", "")
            if explanation and explanation.strip():
                st.markdown("### 🤖 Respuesta de Genie")
                # Split long explanations into paragraphs for better readability
                if len(explanation) > 200:
                    paragraphs = explanation.split(". ")
                    formatted_explanation = ""
                    current_paragraph = ""

                    for sentence in paragraphs:
                        if len(current_paragraph + sentence) > 150:
                            if current_paragraph:
                                formatted_explanation += (
                                    current_paragraph.strip() + ".\n\n"
                                )
                            current_paragraph = sentence
                        else:
                            current_paragraph += sentence + ". "

                    if current_paragraph:
                        formatted_explanation += current_paragraph.strip()

                    st.markdown(formatted_explanation)
                else:
                    st.markdown(explanation)

            # Display SQL query with better formatting
            sql_query = response.get("sql_query", "")
            if sql_query and sql_query.strip():
                st.markdown("### 📝 Consulta SQL Generada")

                # Format the SQL query
                formatted_sql = self.format_sql_query(sql_query)

                # Display in an expandable section for long queries
                if len(formatted_sql) > 200:
                    with st.expander("Ver consulta SQL completa", expanded=False):
                        st.code(formatted_sql, language="sql")
                else:
                    st.code(formatted_sql, language="sql")

            # Display results with enhanced formatting
            result = response.get("result", {})
            data = result.get("data", [])

            if data:
                st.markdown("### 📊 Resultados")

                # Debug info (can be removed later)
                logger.info(f"Data type: {type(data)}")
                logger.info(f"Data length: {len(data) if data else 0}")
                logger.info(f"First item type: {type(data[0]) if data else 'None'}")
                logger.info(
                    f"Columns in result: {result.get('columns', 'No columns found')}"
                )

                # Convert data to DataFrame for better display
                columns = result.get("columns", [])
                logger.info(f"Columns extracted from result: {columns}")

                # Try to create DataFrame in different ways
                df = None

                if columns and data:
                    if isinstance(data[0], list):
                        # Array format - convert to DataFrame
                        logger.info(f"Processing as array format with columns: {columns}")
                        logger.info(f"Data sample: {data[:2] if len(data) >= 2 else data}")
                        
                        # Ensure we have the right number of columns for the data
                        if len(columns) == len(data[0]):
                            df = pd.DataFrame(data, columns=columns)
                            logger.info(f"Created DataFrame with proper column names: {list(df.columns)}")
                        else:
                            logger.warning(f"Column count mismatch: {len(columns)} columns vs {len(data[0])} data columns")
                            # Use available columns up to data width, pad with generic names if needed
                            data_width = len(data[0])
                            adjusted_columns = columns[:data_width]
                            if len(adjusted_columns) < data_width:
                                adjusted_columns.extend([f"Column_{i + 1}" for i in range(len(adjusted_columns), data_width)])
                            df = pd.DataFrame(data, columns=adjusted_columns)
                            logger.info(f"Created DataFrame with adjusted columns: {list(df.columns)}")

                        # Convert string numbers to actual numeric types
                        for col in df.columns:
                            # Try to convert to numeric, keep as string if conversion fails
                            numeric_series = pd.to_numeric(df[col], errors="coerce")
                            if (
                                not numeric_series.isna().all()
                            ):  # If any values converted successfully
                                df[col] = numeric_series.fillna(
                                    df[col]
                                )  # Fill failed conversions with original

                    elif isinstance(data[0], dict):
                        # Dictionary format
                        logger.info("Processing as dictionary format")
                        df = pd.DataFrame(data)

                        # Convert string numbers to actual numeric types
                        for col in df.columns:
                            numeric_series = pd.to_numeric(df[col], errors="coerce")
                            if (
                                not numeric_series.isna().all()
                            ):  # If any values converted successfully
                                df[col] = numeric_series.fillna(
                                    df[col]
                                )  # Fill failed conversions with original
                    else:
                        logger.warning(f"Unknown data format: {type(data[0])}")
                elif data and not columns:
                    logger.warning("No columns found, attempting to infer structure")
                    # Try to infer structure
                    if isinstance(data[0], list):
                        # Create generic column names as last resort
                        num_cols = len(data[0]) if data else 0
                        columns = [f"Column_{i + 1}" for i in range(num_cols)]
                        logger.warning(f"Created generic columns as fallback: {columns}")
                        df = pd.DataFrame(data, columns=columns)

                        # Convert string numbers to actual numeric types
                        for col in df.columns:
                            numeric_series = pd.to_numeric(df[col], errors="coerce")
                            if (
                                not numeric_series.isna().all()
                            ):  # If any values converted successfully
                                df[col] = numeric_series.fillna(
                                    df[col]
                                )  # Fill failed conversions with original

                    elif isinstance(data[0], dict):
                        df = pd.DataFrame(data)

                        # Convert string numbers to actual numeric types
                        for col in df.columns:
                            numeric_series = pd.to_numeric(df[col], errors="coerce")
                            if (
                                not numeric_series.isna().all()
                            ):  # If any values converted successfully
                                df[col] = numeric_series.fillna(
                                    df[col]
                                )  # Fill failed conversions with original
                    else:
                        logger.warning("Could not infer data structure")
                else:
                    logger.warning(f"No data to display: data={bool(data)}, columns={bool(columns)}")

                if df is not None:
                    logger.info(
                        f"Successfully created DataFrame with shape: {df.shape}"
                    )

                    # Format the DataFrame for better display
                    df_display = self.format_dataframe_for_display(df)

                    # Display the table with enhanced styling
                    if len(df) <= 100:
                        st.markdown("#### 📋 Tabla de Datos")
                        st.dataframe(
                            df_display,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                col: st.column_config.TextColumn(
                                    width="medium", help=f"Columna: {col}"
                                )
                                for col in df_display.columns
                            },
                        )
                    else:
                        st.warning(
                            f"⚠️ Conjunto de resultados grande ({len(df)} filas). Mostrando las primeras 100 filas."
                        )
                        st.markdown("#### 📋 Tabla de Datos (Primeras 100 filas)")
                        st.dataframe(
                            df_display.head(100),
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                col: st.column_config.TextColumn(
                                    width="medium", help=f"Columna: {col}"
                                )
                                for col in df_display.columns
                            },
                        )

                    # Display enhanced summary statistics
                    numeric_columns = df.select_dtypes(include=["number"]).columns
                    if len(numeric_columns) > 0:
                        st.markdown("#### 📈 Estadísticas Resumen")

                        # Create dynamic columns based on available metrics
                        metrics_cols = st.columns(min(4, len(numeric_columns) + 1))

                        with metrics_cols[0]:
                            st.metric("Total de Filas", f"{len(df):,}")

                        # Display metrics for up to 3 numeric columns
                        for i, col in enumerate(numeric_columns[:3]):
                            if i + 1 < len(metrics_cols):
                                with metrics_cols[i + 1]:
                                    total_value = df[col].sum()
                                    avg_value = df[col].mean()

                                    # Format large numbers appropriately
                                    if total_value > 1000000:
                                        total_display = f"${total_value / 1000000:.2f}M"
                                    elif total_value > 1000:
                                        total_display = f"${total_value:,.0f}"
                                    else:
                                        total_display = f"{total_value:.2f}"

                                    if avg_value > 1000000:
                                        avg_display = f"${avg_value / 1000000:.2f}M"
                                    elif avg_value > 1000:
                                        avg_display = f"${avg_value:,.0f}"
                                    else:
                                        avg_display = f"{avg_value:.2f}"

                                    st.metric(
                                        f"Total {col}",
                                        total_display,
                                        help=f"Suma total de {col}",
                                    )
                                    st.metric(
                                        f"Promedio {col}",
                                        avg_display,
                                        help=f"Valor promedio de {col}",
                                    )

                        # Show data distribution for the first numeric column
                        if len(numeric_columns) > 0:
                            first_numeric = numeric_columns[0]
                            st.markdown(f"#### 📊 Distribución de {first_numeric}")

                            # Create a simple bar chart for top values
                            if len(df) <= 20:  # For small datasets, show all values
                                chart_data = df.set_index(df.columns[0])[first_numeric]
                                st.bar_chart(chart_data)
                            else:  # For larger datasets, show top 10
                                top_10 = df.nlargest(10, first_numeric)
                                chart_data = top_10.set_index(top_10.columns[0])[
                                    first_numeric
                                ]
                                st.bar_chart(chart_data)
                                st.caption("Mostrando los 10 valores más altos")

                    # Optional: Show raw JSON data in collapsible section
                    with st.expander(
                        "📄 Ver datos originales en formato JSON", expanded=False
                    ):
                        st.json(data)

                else:
                    # Fallback: display as formatted JSON if DataFrame creation failed
                    logger.warning(
                        "Could not create DataFrame, falling back to JSON display"
                    )
                    st.warning(
                        "⚠️ No se pudo procesar los datos como tabla. Mostrando datos en formato JSON."
                    )

                    with st.expander(
                        "📄 Ver datos en formato JSON (opcional)", expanded=False
                    ):
                        st.json(data)

                # Display summary with better formatting
                summary = result.get("summary", "")
                if summary:
                    st.markdown("#### ℹ️ Resumen")
                    st.info(summary)

            else:
                st.warning("⚠️ No se devolvieron datos de la consulta")

        except Exception as e:
            st.error(f"❌ Error mostrando resultados: {str(e)}")
            logger.error(f"Error formatting response for Streamlit: {e}")

    def handle_query(self, query: str):
        """Handle a user query and display results"""
        if not query.strip():
            st.warning("⚠️ Por favor ingresa una pregunta")
            return

        if not self.client:
            st.error("❌ Cliente de Databricks no inicializado")
            return

        try:
            with st.spinner("🤔 Pensando... (esto puede tomar un momento)"):
                # Send query to Genie
                response = self.client.query_genie(query)

                # Store in session state
                if "chat_history" not in st.session_state:
                    st.session_state.chat_history = []

                st.session_state.chat_history.append(
                    {
                        "query": query,
                        "response": response,
                        "timestamp": pd.Timestamp.now(),
                    }
                )

                # Display the response
                self.format_response_for_streamlit(response)

        except Exception as e:
            st.error(f"❌ Error procesando consulta: {str(e)}")
            logger.error(f"Error processing query: {e}")

    def display_chat_history(self):
        if "chat_history" not in st.session_state or not st.session_state.chat_history:
            return

        st.markdown("---")
        st.markdown("### 📝 Historial de Chat")

        # Get the last 10 chats for display
        recent_chats = list(reversed(st.session_state.chat_history[-10:]))

        for i, chat in enumerate(recent_chats):
            # Expand the first 2 chats (most recent), collapse the rest
            is_expanded = i < 2

            # Create a shorter title for the expander
            chat_title = f"🕐 {chat['timestamp'].strftime('%H:%M:%S')} - {chat['query'][:50]}{'...' if len(chat['query']) > 50 else ''}"

            with st.expander(chat_title, expanded=is_expanded):
                st.markdown(f"**Pregunta:** {chat['query']}")

                explanation = chat["response"].get("explanation", "")
                if explanation:
                    st.markdown(f"**Respuesta:** {explanation}")

                sql_query = chat["response"].get("sql_query", "")
                if sql_query:
                    st.code(sql_query, language="sql")

                # Show result summary if available
                result = chat["response"].get("result", {})
                summary = result.get("summary", "")
                if summary:
                    st.info(f"ℹ️ {summary}")

    def run(self):
        # Configure page
        st.set_page_config(
            page_title="Appinnova: UCM Big Data Master",
            page_icon="🎓",
            layout="wide",
            initial_sidebar_state="expanded",
        )

        # Header
        st.title("🎓 Appinnova: UCM Big Data Master")
        st.markdown("¡Haz preguntas sobre tus datos en lenguaje natural!")

        # Initialize client
        if not self.initialize_client():
            st.stop()

        # Display configuration info
        self.display_config_info()

        # Display sample queries
        self.display_sample_queries()

        # Main chat interface
        st.markdown("### 💬 Haz una Pregunta")

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
                "Ingresa tu pregunta:",
                value=default_value,
                placeholder="ej., Muéstrame los 10 mejores clientes por ingresos",
                key="query_text_input",
            )

        with col2:
            submit_button = st.button("🚀 Preguntar", type="primary")

        # Handle query submission (both Enter key and button click)
        if (
            submit_button or (query and query != st.session_state.last_submitted_query)
        ) and query.strip():
            st.session_state.last_submitted_query = query
            self.handle_query(query)

        # Display chat history
        self.display_chat_history()

        # Footer
        st.markdown("---")
        st.markdown("*UCM Big Data Master - Powered by Databricks Genie and Streamlit*")


def main():
    app = StreamlitGenieChat()
    app.run()


if __name__ == "__main__":
    main()
