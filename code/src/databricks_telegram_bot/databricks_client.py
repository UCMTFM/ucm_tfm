import time
from typing import Dict, Any, Optional, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
from loguru import logger
from .config import DatabricksConfig


class DatabricksGenieClient: 
    def __init__(self, config: DatabricksConfig):
        self.config = config
        self.client = WorkspaceClient(
            host=config.workspace_url,
            token=config.access_token
        )
        self.genie_api = GenieAPI(self.client.api_client)
        self.space_id = None
        self.conversation_id = None
        self.catalog = config.catalog
        self.schema = config.schema

    def _get_or_create_space(self) -> str:
        """Get the first available space or create a default one"""
        if self.space_id:
            return self.space_id

        try:
            # List available spaces
            spaces_response = self.genie_api.list_spaces()
            if spaces_response.spaces and len(spaces_response.spaces) > 0:
                self.space_id = spaces_response.spaces[0].space_id
                logger.info(f"Using existing Genie space: {self.space_id}")
                return self.space_id
            else:
                logger.warning("No Genie spaces found. Please create a space in Databricks.")
                raise Exception("No Genie spaces available. Please create a space in Databricks first.")
        except Exception as e:
            logger.error(f"Error getting Genie spaces: {e}")
            raise

    def _get_or_create_conversation(self, space_id: str) -> str:
        """Get or create a conversation for the current session"""
        if self.conversation_id:
            return self.conversation_id

        try:
            # Start a new conversation
            conversation_response = self.genie_api.start_conversation_and_wait(
                space_id=space_id,
                content="Hello, I'm ready to help with data queries."
            )
            self.conversation_id = conversation_response.conversation_id
            logger.info(f"Started new Genie conversation: {self.conversation_id}")
            return self.conversation_id
        except Exception as e:
            logger.error(f"Error starting Genie conversation: {e}")
            raise

    def query_genie(self, question: str, catalog: Optional[str] = None,
                         schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Send a natural language query to Databricks Genie
        """
        try:
            logger.info(f"Sending query to Genie: {question}")
            
            space_id = self._get_or_create_space()
            conversation_id = self._get_or_create_conversation(space_id)

            message_response = self.genie_api.create_message_and_wait(
                space_id=space_id,
                conversation_id=conversation_id,
                content=question
            )

            response_data = self._create_initial_response(question, message_response)
            logger.info(f"Response data: {message_response}")

            if hasattr(message_response, 'attachments') and message_response.attachments:
                logger.info(f"Found {len(message_response.attachments)} attachments")
                self._process_attachments(message_response, space_id, conversation_id, response_data)

            logger.info(f"Successfully received response from Genie")
            return response_data
            
        except Exception as e:
            logger.error(f"Unexpected error in query_genie: {e}")
            raise
    
    def _create_initial_response(self, question: str, message_response) -> Dict[str, Any]:
        """Create the initial response structure"""
        return {
            "query": question,
            "sql_query": "",
            "result": {
                "data": [],
                "summary": "Query processed by Genie"
            },
            "explanation": message_response.content if hasattr(message_response, 'content') else "Processed by Genie"
        }

    def _process_attachments(self, message_response, space_id: str, conversation_id: str, response_data: Dict[str, Any]):
        """Process all attachments in the message response"""
        for i, attachment in enumerate(message_response.attachments):
            logger.info(f"Processing attachment {i+1}: {type(attachment)}")

            self._process_text_attachment(attachment, response_data)
            self._process_query_attachment(attachment, message_response, space_id, conversation_id, response_data)
            self._process_unknown_attachment(attachment)

    def _process_text_attachment(self, attachment, response_data: Dict[str, Any]):
        """Process text attachments (explanations, clarifications, etc.)"""
        if hasattr(attachment, 'text') and attachment.text:
            logger.info(f"Found text attachment: {attachment.text}")
            if hasattr(attachment.text, 'content'):
                text_content = attachment.text.content
                logger.info(f"Text attachment content: {text_content}")
                response_data["explanation"] = text_content

    def _process_query_attachment(self, attachment, message_response, space_id: str, conversation_id: str, response_data: Dict[str, Any]):
        """Process query attachments and execute SQL queries"""
        if hasattr(attachment, 'query') and attachment.query:
            logger.info(f"Found query attachment: {attachment.query}")
            try:
                query_result = self._execute_and_wait_for_query(
                    attachment, message_response, space_id, conversation_id
                )
                self._extract_query_results(query_result, attachment, response_data)
            except Exception as e:
                logger.warning(f"Could not execute query attachment: {e}")
                response_data["result"]["summary"] = f"Error executing query: {str(e)}"

    def _execute_and_wait_for_query(self, attachment, message_response, space_id: str, conversation_id: str):
        """Execute a query and wait for it to complete"""
        logger.info(f"Executing query attachment: {attachment.attachment_id}")
        query_result = self.genie_api.execute_message_attachment_query(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_response.id,
            attachment_id=attachment.attachment_id
        )
        
        logger.info(f"Initial query result: {query_result}")
        logger.info(f"Query result type: {type(query_result)}")
        
        if self._is_query_pending(query_result):
            logger.info("Query is pending, waiting for completion...")
            query_result = self._wait_for_query_completion(
                message_response, attachment, space_id, conversation_id
            )
        else:
            logger.info("Query is not pending, proceeding with current result")
        
        return query_result

    def _is_query_pending(self, query_result) -> bool:
        """Check if a query is still in pending state"""
        try:
            logger.info(f"Checking if query is pending...")
            logger.info(f"Statement response: {query_result.statement_response}")
            logger.info(f"Status: {query_result.statement_response.status}")
            logger.info(f"State: {query_result.statement_response.status.state}")
            logger.info(f"State value: {query_result.statement_response.status.state.value}")
            
            is_pending = query_result.statement_response.status.state.value == 'PENDING'
            logger.info(f"Query is pending: {is_pending}")
            return is_pending
        except Exception as e:
            logger.error(f"Error checking query pending state: {e}")
            return False

    def _wait_for_query_completion(self, message_response, attachment, space_id: str, conversation_id: str):
        """Wait for a query to complete by polling its status"""
        max_wait_time = 60
        wait_time = 0
        check_interval = 15  # Check every 15 seconds
        
        logger.info(f"Starting to wait for query completion...")
        
        while wait_time < max_wait_time:
            time.sleep(check_interval)
            wait_time += check_interval
            
            logger.info(f"Polling query result (attempt {wait_time//check_interval})...")
            
            try:
                updated_result = self.genie_api.get_message_attachment_query_result(
                    space_id=space_id,
                    conversation_id=conversation_id,
                    message_id=message_response.id,
                    attachment_id=attachment.attachment_id
                )
                
                # logger.info(f"Updated query result: {updated_result}")
                
                state = self._get_query_state(updated_result)
                if state:
                    logger.info(f"Query state: {state}")
                    
                    if state == 'SUCCEEDED':
                        logger.info("Query finished successfully!")
                        logger.info("About to return the result...")
                        return updated_result
                    elif state in ['FAILED', 'CANCELED', 'CLOSED']:
                        logger.warning(f"Query {state.lower()}")
                        logger.info("About to return the result due to failure...")
                        return updated_result
                    elif state == 'RUNNING':
                        logger.info("Query is still running, continuing to wait...")
                    elif state == 'PENDING':
                        logger.info("Query is still pending, continuing to wait...")
                    else:
                        logger.info(f"Unknown state: {state}, continuing to wait...")
                else:
                    logger.warning("Could not determine query state")
                    
            except Exception as e:
                logger.error(f"Error polling query result: {e}")
        
        logger.warning("Query timed out after 60 seconds")
        return None

    def _get_query_state(self, query_result) -> Optional[str]:
        """Extract the query state from the result"""
        try: 
            state_value = query_result.statement_response.status.state.value
            logger.info(f"Extracted state value: '{state_value}'")
            return state_value
        except Exception as e:
            logger.error(f"Error getting query state: {e}")
        logger.warning("Could not extract query state")
        return None

    def _extract_query_results(self, query_result, attachment, response_data: Dict[str, Any]):
        """Extract data and metadata from a completed query result"""
        logger.info(f"Extracting query results from: {query_result}")
        
        if not query_result:
            logger.warning("No query result to extract from")
            return
        
        if not hasattr(query_result, 'statement_response'):
            logger.warning("Query result has no statement_response")
            return
        
        statement_response = query_result.statement_response
        logger.info(f"Statement response: {statement_response}")
        
        if not hasattr(statement_response, 'result') or not statement_response.result:
            logger.warning("Statement response has no result")
            return
        
        result_data = statement_response.result
        logger.info(f"Result data: {result_data}")
        
        # Extract data array
        if hasattr(result_data, 'data_array') and result_data.data_array:
            logger.info(f"Found data array with {len(result_data.data_array)} rows")
            response_data["result"]["data"] = result_data.data_array
        else:
            logger.warning("No data array found in result")
        
        # Extract column names
        if hasattr(result_data, 'manifest') and result_data.manifest:
            if hasattr(result_data.manifest, 'schema') and result_data.manifest.schema:
                columns = [col.name for col in result_data.manifest.schema.columns]
                logger.info(f"Found columns: {columns}")
                response_data["result"]["columns"] = columns
        
        # Extract SQL query
        if hasattr(attachment, 'query') and attachment.query:
            logger.info(f"Extracting SQL query: {attachment.query}")
            # Check if attachment.query is a GenieQueryAttachment object
            if hasattr(attachment.query, 'query'):
                # Extract the actual SQL string from the GenieQueryAttachment
                sql_string = attachment.query.query
                logger.info(f"Extracted SQL string: {sql_string}")
                response_data["sql_query"] = sql_string
            else:
                # It's already a string
                response_data["sql_query"] = attachment.query
        
        # Update summary
        if response_data["result"]["data"]:
            response_data["result"]["summary"] = f"Query returned {len(response_data['result']['data'])} rows"
            logger.info(f"Updated summary: {response_data['result']['summary']}")
        else:
            response_data["result"]["summary"] = "Query completed but no data returned"
            logger.warning("Query completed but no data returned")

    def _process_unknown_attachment(self, attachment):
        """Process attachments of unknown type for debugging"""
        if not hasattr(attachment, 'text') and not hasattr(attachment, 'query'):
            try:
                attachment_dict = attachment.as_dict()
                logger.info(f"Unknown attachment type, converting to dict: {attachment_dict}")
            except Exception as e:
                logger.warning(f"Could not convert attachment to dict: {e}")
                logger.info(f"Attachment string representation: {str(attachment)}")

    def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """
        Check the status of a running query
        
        Args:
            query_id: The ID of the query to check
            
        Returns:
            Dictionary containing the query status
        """
        try:
            if not self.space_id or not self.conversation_id:
                return {
                    "query_id": query_id,
                    "status": "UNKNOWN",
                    "message": "No active conversation"
                }

            # Get the message status
            message = self.genie_api.get_message(
                space_id=self.space_id,
                conversation_id=self.conversation_id,
                message_id=query_id
            )

            return {
                "query_id": query_id,
                "status": message.status if hasattr(message, 'status') else "UNKNOWN",
                "message": message.content if hasattr(message, 'content') else "No message content"
            }
                
        except Exception as e:
            logger.error(f"Error checking query status: {e}")
            raise
    
    def list_tables(self, catalog: Optional[str] = None, schema: Optional[str] = None) -> List[str]:
        """List all tables in the specified catalog and schema."""
        catalog = catalog or self.catalog
        schema = schema or self.schema
        try:
            tables = []
            for table in self.client.tables.list(catalog_name=catalog, schema_name=schema):
                tables.append(table.name)
            logger.info(f"Found tables in {catalog}.{schema}: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            raise

    def get_available_tables(self, catalog: Optional[str] = None,
                                 schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get available tables in the specified catalog and schema
        
        Args:
            catalog: Optional catalog name (defaults to config)
            schema: Optional schema name (defaults to config)
            
        Returns:
            Dictionary containing available tables with formatted names
        """
        try:
            catalog = catalog or self.catalog
            schema = schema or self.schema

            # Get table names using the improved method
            table_names = self.list_tables(catalog, schema)

            # Format table names for better display
            formatted_tables = []
            for table_name in table_names:
                # Convert underscores to spaces and capitalize words
                formatted_name = table_name.replace('_', ' ').title()
                formatted_tables.append({
                    "schema_name": schema,
                    "name": table_name,
                    "display_name": formatted_name,
                    "full_name": f"{catalog}.{schema}.{table_name}"
                })

            return {"tables": formatted_tables}
                
        except Exception as e:
            logger.error(f"Error getting available tables: {e}")
            raise
    
    def format_genie_response(self, response: Dict[str, Any]) -> str:
        """
        Format the Genie response into a readable message
        """
        try:
            query = response.get("query", "")
            sql_query = response.get("sql_query", "")
            result = response.get("result", {})
            explanation = response.get("explanation", "")
            
            # Helper function to escape Markdown characters
            def escape_markdown(text: str) -> str:
                if not text:
                    return ""
                text = str(text)
                text = text.replace('_', '\\_')  # Escape underscore
                text = text.replace('*', '\\*')  # Escape asterisk
                text = text.replace('[', '\\[')  # Escape bracket
                text = text.replace(']', '\\]')  # Escape bracket
                text = text.replace('(', '\\(')  # Escape parenthesis
                text = text.replace(')', '\\)')  # Escape parenthesis
                text = text.replace('~', '\\~')  # Escape tilde
                text = text.replace('`', '\\`')  # Escape backtick
                text = text.replace('>', '\\>')  # Escape greater than
                text = text.replace('#', '\\#')  # Escape hash
                text = text.replace('+', '\\+')  # Escape plus
                text = text.replace('-', '\\-')  # Escape minus
                text = text.replace('=', '\\=')  # Escape equals
                text = text.replace('|', '\\|')  # Escape pipe
                text = text.replace('{', '\\{')  # Escape curly brace
                text = text.replace('}', '\\}')  # Escape curly brace
                text = text.replace('.', '\\.')  # Escape dot
                text = text.replace('!', '\\!')  # Escape exclamation
                return text
            
            # Start building the response
            formatted_response = ""
            
            # Add explanation if available (this is Genie's response)
            if explanation and explanation.strip():
                escaped_explanation = escape_markdown(explanation.strip())
                formatted_response += f"{escaped_explanation}\n\n"
            
            # Add SQL query if available
            if sql_query and sql_query.strip():
                formatted_response += f"**SQL Query:**\n```sql\n{sql_query.strip()}\n```\n\n"
            
            # Add results if available
            data = result.get("data", [])
            if data:
                formatted_response += "**Results:**\n"
                
                # Check if data is a list of lists (array format) or list of dicts
                if data and isinstance(data[0], list):
                    # Array format - convert to table format
                    columns = result.get("columns", [])
                    if columns:
                        # Create a simple table with key columns
                        key_columns = ['NumeroFactura', 'Total', 'RazonSocialCliente', 'Fecha']
                        available_columns = [col for col in key_columns if col in columns]
                        
                        if available_columns:
                            # Create header
                            header = " | ".join(available_columns)
                            separator = "---|" * (len(available_columns) - 1) + "---"
                            formatted_response += f"`{header}`\n`{separator}`\n"
                            
                            # Add data rows
                            for row in data:
                                row_values = []
                                for col in available_columns:
                                    col_index = columns.index(col)
                                    if col_index < len(row):
                                        value = str(row[col_index])
                                        # Truncate long values
                                        if len(value) > 30:
                                            value = value[:27] + "..."
                                        row_values.append(value)
                                    else:
                                        row_values.append("")
                                formatted_response += f"`{' | '.join(row_values)}`\n"
                        else:
                            # No key columns found, show first few columns
                            max_cols = min(5, len(columns))
                            header = " | ".join(columns[:max_cols])
                            separator = "---|" * (max_cols - 1) + "---"
                            formatted_response += f"`{header}`\n`{separator}`\n"
                            
                            for row in data:
                                row_values = []
                                for i in range(max_cols):
                                    if i < len(row):
                                        value = str(row[i])
                                        if len(value) > 30:
                                            value = value[:27] + "..."
                                        row_values.append(value)
                                    else:
                                        row_values.append("")
                                formatted_response += f"`{' | '.join(row_values)}`\n"
                    else:
                        # No columns, show first row as example
                        if data[0]:
                            formatted_response += f"Found {len(data)} row(s) of data\n"
                            formatted_response += f"Sample: `{str(data[0][:3])}`...\n"
                else:
                    # Dictionary format
                    if len(data) <= 5:
                        for i, row in enumerate(data, 1):
                            formatted_response += f"**Row {i}:**\n"
                            for key, value in row.items():
                                formatted_response += f"  {key}: `{str(value)}`\n"
                    else:
                        formatted_response += f"Found {len(data)} rows of data\n"
                        formatted_response += f"Sample row: `{str(list(data[0].items())[:3])}`...\n"
                
                # Add summary
                summary = result.get("summary", "")
                if summary:
                    formatted_response += f"\n{escape_markdown(summary)}"
            
            return formatted_response.strip()
            
        except Exception as e:
            logger.error(f"Error formatting response: {e}")
            try:
                return f"Error formatting response: {str(e)}"
            except:
                return "Error formatting response"