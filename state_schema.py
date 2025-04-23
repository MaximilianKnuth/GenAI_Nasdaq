from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class AgentState(BaseModel):
    """State for the multi-agent system"""
    first_run: bool = True  # Track whether this is the first run
    # Input fields
    user_query: str = Field(description="The original user query")
    
    # Task classification fields
    task_type: Optional[str] = Field(None, description="The classified task type (convert_datetime, join_tables, etc.)")
    
    # Next step
    routing_decision: Optional[str] = Field(None, description="Decision made by the router node")
    
    # Data transformation fields
    table_name: Optional[str] = Field(None, description="The name of the table to be processed")
    datetime_columns: Optional[str] = Field(None, description="The datetime columns to be transformed")
    original_timezone: Optional[str] = Field(None, description="The original timezone")
    target_timezone: Optional[str] = Field(None, description="The target timezone")
    
    # RAG results
    rag_results: Optional[str] = Field(None, description="Results from the RAG system")
    
    # Completeness check
    completeness_check_result: Optional[bool] = Field(False, description="Result of the completeness check")
    
    # Code generation
    execution_summary: Optional[Dict[str, Any]] = Field(None, description="Summary of the execution plan")
    generated_code: Optional[str] = Field(None, description="The generated Python code")
    
    # Dataframes dictionary (not part of the serialized state)
    df_dict: Optional[Dict[str, Any]] = Field(None, description="Dictionary of dataframes", exclude=True)
    
    # API Keys (excluded from serialization)
    api_key: Optional[str] = Field(None, description="API key for LLM services", exclude=True)
    openai_api_key: Optional[str] = Field(None, description="OpenAI API key", exclude=True)
    
    # Human interaction
    needs_human_input: bool = Field(False, description="Flag indicating if human input is needed")
    human_message: Optional[str] = Field(None, description="Message to show to human when requesting input")
    human_response: Optional[str] = Field(None, description="Response from human")
    historical_response: Optional[str] = Field(None, description="Previous Response from human")
    
    missing_info_type: Optional[str] = Field(None, description="Type of missing information: 'table', 'datetime', 'timezone'")
    
    # Error handling
    error: Optional[str] = Field(None, description="Error message, if any")