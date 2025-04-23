import pandas as pd
import os
import pytz
from openai import OpenAI
from typing import Dict, List, Optional, Tuple, Any
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

# Import your existing validation and RAG components
from Agents.data_validation_agent import DataValidator
from Agents.date_validation_agent import DateValidationAgent
from Agents.retrieval_agent import RAG_retrieval
from Agents.chunk_table import trunk_table_execute
from state_schema import AgentState
import zoneinfo
import json, textwrap
import logging

# small cache so we don't ask the LLM twice for the same text
_TZ_CACHE: dict[str, tuple[str | None, str | None]] = {}

def _canonical_tz(name: str) -> str | None:
    print(f"Canonicalizing timezone: {name}")
    """
    Return a valid IANA tz string or None.
    Accepts aliases like EST, EDT, PST, CET, etc.
    """
    alias_map = {
        "EST": "US/Eastern",
        "EDT": "US/Eastern",
        "CST": "US/Central",
        "CDT": "US/Central",
        "MST": "US/Mountain",
        "MDT": "US/Mountain",
        "PST": "US/Pacific",
        "PDT": "US/Pacific",
        "CET": "Europe/Berlin",
        "CEST": "Europe/Berlin",
        "GMT": "Etc/GMT",
    }
    cand = alias_map.get(name.upper(), name)
    try:
        zoneinfo.ZoneInfo(cand)
        return cand
    except Exception:
        return None

def _ask_llm_for_timezones(
    user_query: str, client: OpenAI
) -> tuple[str | None, str | None]:
    """
    Use DeepSeek / GPT to extract *two* IANA time‑zones from free text.
    Returns (orig_tz, target_tz) or (None, None) if uncertain.
    """
    if user_query in _TZ_CACHE:
        return _TZ_CACHE[user_query]

    system_msg = (
        "You are a time‑zone normaliser. "
        "Extract exactly TWO IANA time‑zone identifiers from the text I give you. "
        "Return them comma‑separated, no extra words. "
        "If you cannot find both with confidence, reply 'NONE'."
    )

    rsp = client.chat.completions.create(
        model="deepseek-chat",
        temperature=0,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_query},
        ],
    )

    content = rsp.choices[0].message.content.strip()

    if content.upper() == "NONE":
        _TZ_CACHE[user_query] = (None, None)
        return None, None

    pieces = [p.strip() for p in content.split(",")[:2]]
    tz_1 = _canonical_tz(pieces[0]) if len(pieces) > 0 else None
    tz_2 = _canonical_tz(pieces[1]) if len(pieces) > 1 else None

    _TZ_CACHE[user_query] = (tz_1, tz_2)
    return tz_1, tz_2

# Task Classification Agent
def task_classification_agent(state: AgentState) -> Dict[str, Any]:
    print("task_classification_agent")
    """Classify the task based on user query"""
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
    
    # Define the classification prompt
    prompt = f"""
    Classify the following user query into one of these categories:
    - convert_datetime: When the user wants to convert datetime/timestamps between timezones
    - join_tables: When the user wants to join two or more tables together
    - check_distribution: When the user wants to analyze data distribution
    
    User query: "{state.user_query}"
    
    Return ONLY the category name, nothing else.
    """
    
    # Call the LLM
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0
    )
    
    task_type = response.choices[0].message.content.strip()
    
    # Update state
    return {"task_type": task_type}

# RAG Agent for table and column extraction if not defined - timezone task
def tz_rag_agent(state: AgentState) -> Dict[str, Any]:
    print("rag_agent")
    """Enhanced RAG agent that incorporates human input when available"""
    pdf_folder = "01_Data/text_data"
    
    try:
        # Instantiate the RAG pipeline
        rag = RAG_retrieval(pdf_folder, state.openai_api_key, state.api_key)
        
        # Build enhanced prompt incorporating human input if available
        combined_query = state.user_query
        if state.human_response:
            combined_query = f"""
            Original query: {state.user_query}
            User clarification: {state.human_response}{state.historical_response}
            
            Based on both the original query and clarification:
            """
        
        prompt = f"""
        Return me the table name and corresponding column name that are of the type datetime the user asked for. 
        If you can't find anything that matches, suggest other table names and column names and explicit name it suggestion.
        **Output Format**
        Exact Matches: Your answer (if we have exact matches)
        Suggestion: If we don't have exact matches, then return the table name and datetime column names that are similar to the task.
        
        User query: {combined_query}
        """
        
        answer = rag.test_pipeline(prompt)
        
        # Parse the RAG output with enhanced context
        parsing_prompt = f"""
        You are a helpful AI assistant to find the corresponding table name and column name based on:
        
        Original query: {state.user_query}
        {"User clarification: " + state.human_response if state.human_response else "" + state.historical_response if state.historical_response else ""}
        
        Based on the returned information from the RAG: {answer}

        ### TASK:
        1. Extract the table name (the table name that the data is currently in).
        2. Extract the column names that where we can transform the datetime (the datetime columns the user wants to convert).
        3. If you can't retrieve the table and column names, return `None`.

        ### IMPORTANT RULES:
        - Do not include any additional text or explanations.
        - If the RAG is giving you suggestions and is not sure about the table or column that the user wants. Return None!

        ### OUTPUT FORMAT:
        Example: `TABLE NAME, COLUMN NAME`
        Example: None
        """

        client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "You are a helpful assistant and only return either the the table and column names or 'None' ."},
                {"role": "user", "content": parsing_prompt},
            ],
            temperature=0
        )

        raw_output = response.choices[0].message.content.strip()
        
        # Process output and construct response as before
        if raw_output.lower() == "none":
            return {
                "needs_human_input": True,
                "human_message": f"I couldn't determine which table and column to use. {answer}\n\nPlease provide more details:",
                "rag_results": answer,
                "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
                "human_response": None  # Clear human response since we've used it
            }

        # Split the output
        output_text = raw_output.split(", ")
        if len(output_text) != 2:
            return {
                "needs_human_input": True,
                "human_message": f"I couldn't parse the table and column information. {answer}\n\nPlease specify which table and column to use:",
                "rag_results": answer,
                "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
                "human_response": None  # Clear human response since we've used it
            }
        else:
            table_name = output_text[0]
            datetime_columns = output_text[1]
            return {
                "table_name": table_name,
                "datetime_columns": datetime_columns,
                "rag_results": answer,
                "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
                "human_response": None  # Clear human response since we've used it
            }

    except Exception as e:
        return {
            "error": f"Error in RAG agent: {str(e)}",
            "needs_human_input": True,
            "human_message": "I encountered an error while trying to identify the table and columns. Please specify which table and column to use:",
            "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
            "human_response": None  # Clear human response since we've used it
        }

def rag_lookup(state: AgentState) -> Dict[str, Any]:
    print("rag_lookup")
    """
    Fill table_name & datetime_column only if they are missing.
    """
    updates: Dict[str, Any] = {}
    openai_api_key='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'
    deepseek_api_key='sk-74c415edef3f4a16b1ef8deb3839cf2a'
    pdf_folder = "01_Data/text_data"
    
    
    if state.table_name and state.datetime_columns:
        return updates   # nothing to do

    rag = RAG_retrieval(pdf_folder="01_Data/text_data", )

    suggestion = rag.test_pipeline(state.user_query)
    # suggestion is expected to be "TABLE, COLUMN" or None
    if suggestion:
        try:
            table, column = [s.strip() for s in suggestion.split(",")]
            if not state.table_name:
                updates["table_name"] = table
            if not state.datetime_columns:
                updates["datetime_columns"] = column
        except ValueError:
            pass  # ignore ill‑formed suggestion

    return updates

# Timezone Extraction Agent
def timezone_extraction_agent(state: AgentState) -> Dict[str, Any]:
    print("timezone_extraction_agent")
    """
    Enhanced timezone extraction that incorporates human input when available
    """
    updates: Dict[str, Any] = {}
    combined_query = state.user_query
    
    # Incorporate human input if available
    if state.human_response:
        combined_query = f"{state.user_query}\nAdditional information: {state.human_response} {state.historical_response}"
        # Clear human response as we're using it
        updates["human_response"] = None
    
    # --- 1. Heuristic scan of the combined text ------------------------------
    for token in combined_query.replace(",", " ").split():
        tz = _canonical_tz(token)
        if tz:
            if not state.original_timezone:
                updates["original_timezone"] = tz
            elif not state.target_timezone:
                updates["target_timezone"] = tz
        if updates.get("original_timezone") and updates.get("target_timezone"):
            break

    # --- 2. If still missing anything, call the LLM with combined context ----------------------
    if not (
        (state.original_timezone or updates.get("original_timezone"))
        and (state.target_timezone or updates.get("target_timezone"))
    ):
        client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
        
        # Enhanced prompt that includes both original query and human response
        tz_extraction_prompt = f"""
        Extract the original timezone and target timezone from this information:
        
        {combined_query}
        
        Return exactly two IANA timezone identifiers, comma separated (e.g., "US/Eastern, UTC").
        If you cannot find both with confidence, reply 'NONE'.
        """
        
        # Call LLM with enhanced context
        rsp = client.chat.completions.create(
            model="deepseek-chat",
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a timezone extraction specialist."},
                {"role": "user", "content": tz_extraction_prompt},
            ],
        )

        content = rsp.choices[0].message.content.strip()

        if content.upper() == "NONE":
            orig, target = None, None
        else:
            pieces = [p.strip() for p in content.split(",")[:2]]
            orig = _canonical_tz(pieces[0]) if len(pieces) > 0 else None
            target = _canonical_tz(pieces[1]) if len(pieces) > 1 else None

        if orig and not (state.original_timezone or updates.get("original_timezone")):
            updates["original_timezone"] = orig
        if target and not (state.target_timezone or updates.get("target_timezone")):
            updates["target_timezone"] = target

    # --- 3. If still incomplete, defer to human --------------------------
    if not (
        (state.original_timezone or updates.get("original_timezone"))
        and (state.target_timezone or updates.get("target_timezone"))
    ):
        # Construct more helpful message based on what we've learned so far
        message = "I couldn't confidently determine both the original and target time‑zones."
        
        if state.original_timezone or updates.get("original_timezone"):
            orig = state.original_timezone or updates.get("original_timezone")
            message += f"\nI found that the original timezone is '{orig}', but I need the target timezone."
        elif state.target_timezone or updates.get("target_timezone"):
            target = state.target_timezone or updates.get("target_timezone")
            message += f"\nI found that the target timezone is '{target}', but I need the original timezone."
        else:
            message += "\nI couldn't determine either the original or target timezone."
        
        message += "\nPlease specify using format: orig_tz=<timezone>, target_tz=<timezone>"
        
        return {
            "needs_human_input": True,
            "human_message": message,
        }

    return updates

def existence_and_column_type_check(state: AgentState) -> Dict[str, Any]: # check if received or stored data is of the right type
    print("existence_and_column_type_check")
    """
    Enhanced completeness check that validates:
    1. Table exists in df_dict
    2. Column exists in the specified table
    3. Column is a datetime column
    4. Provides intelligent suggestions using RAG and LLM
    """
    missing = []
    invalid = []
    suggestions = {}
    updates = {}  # This will store our state updates
    
    # Check if table exists and is valid
    if not state.table_name:
        missing.append("table name")
    elif state.table_name not in state.df_dict:
        invalid.append(f"table '{state.table_name}' not found")
        # Clear the invalid table name from state
        updates["table_name"] = None
        # Store for suggestions
        suggestions["available_tables"] = list(state.df_dict.keys())
    
    # Only check column if table is valid
    if state.table_name and state.table_name in state.df_dict:
        df = state.df_dict[state.table_name]
        
        # Check if column exists
        if not state.datetime_columns:
            missing.append("datetime column")
        elif state.datetime_columns not in df.columns:
            invalid.append(f"column '{state.datetime_columns}' not found in table '{state.table_name}'")
            # Clear the invalid column from state
            updates["datetime_columns"] = None
            # Store for suggestions
            suggestions["available_columns"] = list(df.columns)
            
            # Try to identify potential datetime columns
            datetime_candidates = []
            for col in df.columns:
                try:
                    # Check column name for datetime hints
                    if any(kw in col.lower() for kw in ["time", "date", "dt", "timestamp"]):
                        datetime_candidates.append(col)
                    # Try to parse as datetime
                    elif pd.to_datetime(df[col], errors='coerce').notna().any():
                        datetime_candidates.append(col)
                except:
                    pass
            
            if datetime_candidates:
                suggestions["datetime_column_suggestions"] = datetime_candidates
        else:
            # Column exists, check if it's a datetime type
            try:
                # Try to convert to datetime if not already
                if not pd.api.types.is_datetime64_any_dtype(df[state.datetime_columns]):
                    try:
                        # Attempt to convert the column to datetime
                        test_conversion = pd.to_datetime(df[state.datetime_columns], errors='raise')

                        # Check if the column contains time information (hours and minutes)
                        if (test_conversion.dt.hour == 0).all() and (test_conversion.dt.minute == 0).all():
                            raise ValueError(
                                f"Column '{state.datetime_columns}' contains only dates (Year-Month-Day) without time information. "
                                "Please provide a column with full datetime values (including hours and minutes)."
                            )
                    except Exception as e:
                        # Handle cases where the column cannot be converted to datetime
                        invalid.append(f"column '{state.datetime_columns}' cannot be converted to datetime: {str(e)}")
                        # Clear the invalid datetime column from state
                        updates["datetime_columns"] = None
                    else:
                        # If conversion succeeds it can be transformed to datetime64, 
                        pass
            except Exception as e:
                # Catch any unexpected errors during validation
                invalid.append(f"error validating datetime column: {str(e)}")
                # Clear the invalid datetime column from state
                updates["datetime_columns"] = None
    
    # Timezone validation can also be enhanced 
    # For example, verify they are valid IANA timezones
    if state.original_timezone:
        try:
            zoneinfo.ZoneInfo(state.original_timezone)
        except Exception:
            invalid.append(f"invalid original timezone: '{state.original_timezone}'")
            updates["original_timezone"] = None
    else:
        missing.append("original timezone")
        
    if state.target_timezone:
        try:
            zoneinfo.ZoneInfo(state.target_timezone)
        except Exception:
            invalid.append(f"invalid target timezone: '{state.target_timezone}'")
            updates["target_timezone"] = None
    else:
        missing.append("target timezone")

    # If everything is good, proceed
    if not missing and not invalid:
        return {
            "completeness_check_result": True,
            "needs_human_input": False,
            "human_message": None,
        }
    
    # Generate helpful suggestions
    suggestion_text = generate_smart_suggestions(state, missing, invalid, suggestions)
    
    # Construct helpful error message
    msg = ""
    if invalid:
        msg += f"Found invalid inputs: {', '.join(invalid)}.\n\n"
    if missing:
        msg += f" Missing information: {', '.join(missing)}.\n\n"
    
    msg += suggestion_text
    #msg += "\nPlease provide the corrected information using format: table=X, column=Y, orig_tz=Z, target_tz=W"
    
    # Set the human interaction flags
    updates["needs_human_input"] = True
    updates["human_message"] = msg
    
    return updates

def generate_smart_suggestions(

    state: AgentState, 
    missing: List[str], 
    invalid: List[str], 
    suggestions: Dict[str, Any]
) -> str:
    print("generate_smart_suggestions")
    """
    Generate contextual suggestions using RAG and LLM based on what's missing or invalid.
    """
    result = ""
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
    
    # --- TABLE SUGGESTIONS ---
    if "table name" in missing or any("table" in err for err in invalid):
        # First use available tables from df_dict
        if "available_tables" in suggestions:
            result += f"📊 Available tables: {', '.join(suggestions['available_tables'])}\n\n"
        
        # Then augment with RAG if needed
        try:
            pdf_folder = "01_Data/text_data"
            rag = RAG_retrieval(pdf_folder, state.openai_api_key, state.api_key)
            
            rag_query = f"What tables would be most relevant for this query: {state.user_query}?"
            table_info = rag.test_pipeline(rag_query)
            result += f"💡 Suggested tables based on your query: {table_info}\n\n"
        except Exception as e:
            logging.warning(f"RAG suggestion error for tables: {str(e)}")
    
    # --- COLUMN SUGGESTIONS ---
    if "datetime column" in missing or any("column" in err for err in invalid):
        # First use available columns if we have a valid table
        valid_table = state.table_name and state.table_name in state.df_dict
        
        if valid_table:
            # Show datetime column suggestions if we have them
            if "datetime_column_suggestions" in suggestions and suggestions["datetime_column_suggestions"]:
                result += f"🕒 Potential datetime columns in '{state.table_name}': "
                result += f"{', '.join(suggestions['datetime_column_suggestions'])}\n\n"
            # Otherwise show all columns
            elif "available_columns" in suggestions:
                result += f"🔍 All columns in '{state.table_name}': "
                result += f"{', '.join(suggestions['available_columns'][:10])}"
                if len(suggestions['available_columns']) > 10:
                    result += f" and {len(suggestions['available_columns']) - 10} more"
                result += "\n\n"
        
        # Then try RAG for more context
        try:
            if valid_table:
                pdf_folder = "01_Data/text_data"
                rag = RAG_retrieval(pdf_folder, state.openai_api_key, state.api_key)
                
                rag_query = f"What datetime columns are in the {state.table_name} table that could be used for timezone conversion?"
                column_info = rag.test_pipeline(rag_query)
                result += f"💡 Suggested datetime columns in {state.table_name}: {column_info}\n\n"
        except Exception as e:
            logging.warning(f"RAG suggestion error for columns: {str(e)}")
    
    # --- TIMEZONE SUGGESTIONS ---
    if "original timezone" in missing or "target timezone" in missing:
        # Extract timezone info from query using LLM
        prompt = f"""
        Based on this user query: "{state.user_query}"
        
        Extract and suggest:
        {' and '.join(['original timezone' if 'original timezone' in missing else '', 
                      'target timezone' if 'target timezone' in missing else ''])}
        
        Return only valid IANA timezone identifiers (like US/Eastern, UTC, Europe/Berlin).
        If the query mentions timezone abbreviations like ET, EST, PST, convert them to proper IANA identifiers.
        Suggest the 3 most likely timezones for each missing field based on the query context.
        
        Format: "Original timezone suggestions: X, Y, Z. Target timezone suggestions: A, B, C."
        If you can't find any specific timezone hints, suggest common ones like UTC, US/Eastern, etc.
        """
        
        try:
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are a helpful timezone suggestion assistant."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2
            )
            
            tz_suggestions = response.choices[0].message.content.strip()
            result += f"🌐 {tz_suggestions}\n\n"
        except Exception as e:
            logging.warning(f"Timezone suggestion error: {str(e)}")
            # Minimal fallback
            result += "🌐 Common timezones: UTC, US/Eastern, US/Pacific, Europe/London\n\n"
    
    # If we have an invalid input but couldn't generate specific suggestions, offer a RAG-based analysis
    if invalid and not result:
        try:
            pdf_folder = "01_Data/text_data"
            rag = RAG_retrieval(pdf_folder, state.openai_api_key, state.api_key)
            
            rag_query = f"Given this query: '{state.user_query}', what tables and datetime columns would be most appropriate for timezone conversion?"
            general_info = rag.test_pipeline(rag_query)
            result += f"💡 Based on available data: {general_info}\n\n"
        except Exception as e:
            logging.warning(f"RAG fallback suggestion error: {str(e)}")
    
    return result

def next_step(state: AgentState) -> str:
    print("next_step")
    """
    Return one of: need_table, need_tz, ask_human, done
    """
    if state.needs_human_input:
        return "ask_human"
    if not (state.table_name and state.datetime_columns):
        return "need_table"
    if not (state.original_timezone and state.target_timezone):
        return "need_tz"
    return "done"

# Validation Agent
def validation_agent(state: AgentState) -> Dict[str, Any]:
    print("validation_agent")
    """Validate the data and create execution summary"""
    try:
        # Get the dataframe
        df = state.df_dict[state.table_name]
        if state.datetime_columns not in df.columns:
            return {
                "needs_human_input": True,
                "human_message": (
                    f"Column '{state.datetime_columns}' not found in {state.table_name}. "
                    f"Available columns: {', '.join(df.columns[:10])}…"
                )
            }
        
        # Initialize validators
        data_validator = DataValidator()
        date_validation_agent = DateValidationAgent()
        
        # Run validations
        validation_summary = data_validator.validate_dataframe(df)
        date_validation_result = date_validation_agent.validate_date_column(df, state.datetime_columns)
        
        # Create execution summary
        execution_summary = {
            "Executed Table": state.table_name,
            "columns_converted": state.datetime_columns,
            "original_timezone": state.original_timezone,
            "new_timezone": state.target_timezone,
            "validation_summary": validation_summary,
            "date_validation": date_validation_result
        }
        
        return {"execution_summary": execution_summary}
    
    except Exception as e:
        return {
            "error": f"Error in validation: {str(e)}",
            "needs_human_input": True,
            "human_message": f"I encountered an error while validating the data: {str(e)}\nPlease provide corrected information:"
        }

# Code Generation Agent
def code_generation_agent(state: AgentState) -> Dict[str, Any]:
    """Generate code based on execution summary"""
    print("code_generation_agent")
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
    
    file_path = f"01_Data/{state.table_name}.csv"
    
    # Get sample data for the prompt
    try:
        df = pd.read_csv(file_path, nrows=5)
        sample_data = df.to_markdown(index=False)
    except Exception as e:
        sample_data = f"[Error reading {file_path}: {str(e)}]"
    
    # Generate the prompt for code generation
    prompt = f"""
    You are a Python code generation assistant. The user provided:

    QUERY: "{state.user_query}{state.human_response}{state.historical_response}"

    EXECUTION CONTEXT:
    - Table: {state.table_name}
    - Datetime columns to convert: {state.datetime_columns}
    - Original timezone: {state.original_timezone}
    - Target timezone: {state.target_timezone}

    ### Dataset Preview (first 5 rows):
    {sample_data}

    TASK REQUIREMENTS:
    1. Load the dataset from: '{file_path}'
    2. Convert the datetime column(s) from {state.original_timezone} to {state.target_timezone}
    3. Save the transformed data with '_transformed' suffix
    4. Return ONLY the executable Python code without any Markdown formatting
    5. Include proper error handling
    6. Do not use ```python or ``` markers
    """

    response = client.chat.completions.create(
        model="deepseek-coder",
        messages=[
            {"role": "system", "content": "You are a helpful Python coding assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0
    )
    
    code = response.choices[0].message.content.strip()
    
    # Clean code block markers if present
    if code.startswith('```python') and code.endswith('```'):
        code = code[9:-3].strip()
    elif code.startswith('```') and code.endswith('```'):
        code = code[3:-3].strip()
    
    # Save code to file
    with open("generated_code.py", "w") as f:
        f.write(code)
    
    return {"generated_code": code}

# Human Input Handler
def process_human_input(state: AgentState) -> Dict[str, Any]:
    print("process_human_input")
    """
    Enhanced human input parser that handles more flexible input formats
    and validates against actual dataframes.
    """
    reply = (state.human_response or "").strip()
    if not reply:
        return {}

    updates: Dict[str, Any] = {}
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
    
    # # First try parsing key=value pairs
    pairs_found = False
    # for part in reply.split(","):
    #     if "=" in part:
    #         pairs_found = True
    #         k, v = [p.strip() for p in part.split("=", 1)]
    #         if k.lower() in ("table", "table_name"):
    #             updates["table_name"] = v
    #         elif k.lower() in ("column", "datetime_column", "columns", "datetime_columns"):
    #             updates["datetime_columns"] = v
    #         elif k.lower() in ("orig_tz", "original_timezone", "source_tz", "from_tz"):
    #             updates["original_timezone"] = _canonical_tz(v) or v
    #         elif k.lower() in ("target_tz", "target_timezone", "dest_tz", "to_tz"):
    #             updates["target_timezone"] = _canonical_tz(v) or v
    
    # If no key=value pairs found, try using LLM to parse natural language response
    if not pairs_found and len(reply) > 3:  # Ensure it's not just a short reply
        prompt = f"""
        Extract table name, datetime column, original timezone, and target timezone from this user response:
        
        User query: "{state.user_query} {state.historical_response}"
        User response: "{reply}"
        
        Return a JSON object with these keys: table_name, datetime_columns, original_timezone, target_timezone
        If any values can't be determined, use null.
        """
        
        try:
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that extracts structured data from text."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0
            )
            
            content = response.choices[0].message.content.strip()
            # Extract JSON from content in case there's text around it
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                extracted = json.loads(json_match.group(0))
                
                # Update with extracted values if they're not None
                for k, v in extracted.items():
                    if v is not None and k in ["table_name", "datetime_columns", "original_timezone", "target_timezone"]:
                        # Normalize timezones
                        if k in ["original_timezone", "target_timezone"]:
                            v = _canonical_tz(v) or v
                        updates[k] = v
        except Exception as e:
            logging.warning(f"LLM extraction error: {str(e)}")
    
    # Validate and normalize the extracted values against the actual data
    if "table_name" in updates and updates["table_name"] and state.df_dict:
        # Check if the provided table exists
        if updates["table_name"] not in state.df_dict:
            # Try to find closest match
            closest = None
            min_distance = float('inf')
            for tbl in state.df_dict.keys():
                # Simple string distance (could use Levenshtein but keeping it simple)
                distance = abs(len(tbl) - len(updates["table_name"]))
                if distance < min_distance and updates["table_name"].lower() in tbl.lower() or tbl.lower() in updates["table_name"].lower():
                    min_distance = distance
                    closest = tbl
            
            if closest:
                logging.info(f"Corrected table name from '{updates['table_name']}' to '{closest}'")
                updates["table_name"] = closest
    
    # Clear human flag and mark as no longer first run
    updates.update({
        "needs_human_input": False, 
        "human_message": None,
        "first_run": False  # Mark that we've been through one cycle
    })
    return updates

# Validation check for Join Table extrations
def join_table_check(state: AgentState) -> Dict[str, Any]:
    print("join_table_check")

    missing = []
    invalid = []
    warnings = []
    suggestions = {}
    updates = {}

    t1, t2 = state.table_name1, state.table_name2
    c1, c2 = state.join_column1, state.join_column2

    df_dict = state.df_dict or {}

    # Check table existence
    for tname, table in [("table_name1", t1), ("table_name2", t2)]:
        if not table:
            missing.append(tname)
        elif table not in df_dict:
            invalid.append(f"table '{table}' not found")
            updates[tname] = None
            suggestions[f"{tname}_suggestions"] = list(df_dict.keys())

    # check column existence if table exists
    if t1 in df_dict and t2 in df_dict:
        df1, df2 = df_dict[t1], df_dict[t2]

        # Check join column existence
        for cname, table, df in [(c1, t1, df1), (c2, t2, df2)]:
            if not cname:
                missing.append(f"join column in table '{table}'")
            elif cname not in df.columns:
                invalid.append(f"column '{cname}' not found in table '{table}'")
                updates[f"join_column{'1' if table == t1 else '2'}"] = None
                suggestions[f"{'join_column1' if table == t1 else 'join_column2'}_suggestions"] = list(df.columns)

        # Check type compatibility
        if c1 in df1.columns and c2 in df2.columns:
            type1, type2 = df1[c1].dtype, df2[c2].dtype
            if type1 != type2:
                warnings.append(f"Type mismatch: {t1}.{c1} is {type1}, {t2}.{c2} is {type2}")

        # Check nulls
        if c1 in df1.columns and df1[c1].isna().mean() > 0.1:
            warnings.append(f"{t1}.{c1} has >10% missing values")
        if c2 in df2.columns and df2[c2].isna().mean() > 0.1:
            warnings.append(f"{t2}.{c2} has >10% missing values")

        # Check cardinality (rough estimate of many-to-many)
        unique1 = df1[c1].nunique() if c1 in df1.columns else 0
        unique2 = df2[c2].nunique() if c2 in df2.columns else 0
        if unique1 < len(df1) and unique2 < len(df2):
            warnings.append("This may be a many-to-many join. Expect large row growth.")

    if not missing and not invalid:
        msg = "Join check passed. You may proceed with merging."
        if warnings:
            msg += "\nWarnings:\n" + "\n".join(warnings)
        return {
            "completeness_check_result": True,
            "needs_human_input": False,
            "human_message": msg,
        }
    
    # Generate helpful suggestions
    suggestion_text = generate_smart_suggestions(state, missing, invalid, suggestions)
    
    # Construct helpful error message
    msg = ""
    if invalid:
        msg += f"Found invalid inputs: {', '.join(invalid)}.\n\n"
    if missing:
        msg += f" Missing information: {', '.join(missing)}.\n\n"
    
    msg += suggestion_text
    #msg += "\nPlease provide the corrected information using format: table=X, column=Y, orig_tz=Z, target_tz=W"
    
    # Set the human interaction flags
    updates["needs_human_input"] = True
    updates["human_message"] = msg
    
    return updates

# RAG Agent for table and column extraction if not defined - timezone
def join_table_rag_agent(state: AgentState) -> Dict[str, Any]:
    print("join_table_rag")

    pdf_folder = "01_Data/text_data"

    try:
        rag = RAG_retrieval(pdf_folder, state.openai_api_key, state.api_key)

        known = {
            "table_name1": state.table_name1,
            "join_column1": state.join_column1,
            "table_name2": state.table_name2,
            "join_column2": state.join_column2
        }

        known_str = "\n".join(f"{k}: {v}" for k, v in known.items() if v is not None)

        combined_query = f"""
        User's original query: {state.user_query}
        Known values:
        {known_str}

        Please help fill in the missing table name(s) or join column(s) using the known ones as anchor points.
        """

        rag_prompt = f"""
        Based on the user's query and partial join info, infer any missing values.
        If confident, return: Table1, JoinColumn1, Table2, JoinColumn2
        If unsure or guessing, return: None
        """

        answer = rag.test_pipeline(rag_prompt)

        parsing_prompt = f"""
        Extract exactly 4 values from this RAG output:
        {answer}

        Return: Table1, JoinColumn1, Table2, JoinColumn2
        Or return: None
        """

        client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "Return either four comma-separated values or 'None'."},
                {"role": "user", "content": parsing_prompt},
            ],
            temperature=0
        )

        raw_output = response.choices[0].message.content.strip()

        if raw_output.lower() == "none":
            return {
                "needs_human_input": True,
                "human_message": f"Could not confidently complete the join info. RAG said:\n{answer}\n\nPlease clarify any missing table or column names.",
                "rag_results": answer,
                "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
                "human_response": None
            }

        parts = [s.strip() for s in raw_output.split(",")]
        if len(parts) != 4:
            return {
                "needs_human_input": True,
                "human_message": f"Could not parse 4 values from RAG output: {answer}\n\nPlease clarify the two tables and their join keys.",
                "rag_results": answer,
                "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
                "human_response": None
            }

        return {
            "table_name1": parts[0],
            "join_column1": parts[1],
            "table_name2": parts[2],
            "join_column2": parts[3],
            "rag_results": answer,
            "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
            "human_response": None
        }

    except Exception as e:
        return {
            "error": f"Error in join_table_rag_fill_in: {str(e)}",
            "needs_human_input": True,
            "human_message": "Something went wrong while attempting to complete the join information. Please fill in the missing details.",
            "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
            "human_response": None
        }

def llm_first_pass(state: AgentState) -> Dict[str, Any]:
    print("llm_first_pass")
    
    
#if it's convert datetime task
    if state.task_type == "convert_datetime":
        if all(getattr(state, f) for f in (
            "table_name", "datetime_columns", "original_timezone", "target_timezone")):
            return {}

        client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")

        table_overview = "\n".join(
            f"- Table '{tbl}' has columns: {', '.join(df.columns[:10])}"
            for tbl, df in (state.df_dict or {}).items()
        )

        sys_prompt = f"""
        You are a strict JSON generator. Extract:
        - table_name
        - datetime_column
        - original_timezone (IANA)
        - target_timezone (IANA)
        Use null for unknowns.

        AVAILABLE TABLES:
        {table_overview}
        """

        user_prompt = f"""
        Parse: "{state.user_query}"
        Example: 'Convert timestamps in TableX from ET to UTC'
        Output: {{
            "table_name": "TableX",
            "datetime_column": "timestamps",
            "original_timezone": "US/Eastern",
            "target_timezone": "UTC"
        }}
        """


# if it's join_table task
    elif state.task_type == "join_tables":
        if all(getattr(state, f) for f in (
            "table_name1", "join_column1", "table_name2", "join_column2")):
            return {}

        client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")

        table_overview = "\n".join(
            f"- Table '{tbl}' has columns: {', '.join(df.columns[:10])}"
            for tbl, df in (state.df_dict or {}).items()
        )

        sys_prompt = f"""
        Extract the following for a table join operation:
        - table_name1 (first table name that needs to be joined)
        - join_column1 (the column to join on in first table)
        - table_name2 (second table name that needs to be joined)
        - join_column2 (the column to join on in second table)

        AVAILABLE TABLES:
        {table_overview}
        """

        user_prompt = f"""
        Parse: "{state.user_query}"
        Example: 'Join EFR and EQR based on ticker'
        Output: {{
            "table_name1": "EFR",
            "join_column1": "ticker",
            "table_name2": "EQR",
            "join_column2": "ticker"
        }}
        """

    else:
        return {}

    # Run the LLM
    resp = client.chat.completions.create(
        model="deepseek-chat",
        temperature=0,
        messages=[
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ],
    ).choices[0].message.content.strip()

    try:
        json_start = resp.find('{')
        json_end = resp.rfind('}') + 1
        if json_start >= 0 and json_end > 0:
            json_str = resp[json_start:json_end]
            extracted = json.loads(json_str)
        else:
            extracted = json.loads(resp)
    except Exception as e:
        print(f"llm_first_pass could not parse output: {resp}. Error: {str(e)}")
        return {}

    updates = {}

    if state.task_type == "convert_datetime":
        for k in ("original_timezone", "target_timezone"):
            if tz := extracted.get(k):
                tz_can = _canonical_tz(tz)
                if tz_can and getattr(state, k) is None:
                    updates[k] = tz_can

        tbl = extracted.get("table_name")
        col = extracted.get("datetime_column")
        if tbl and getattr(state, "table_name") is None:
            updates["table_name"] = tbl

        if tbl and tbl in state.df_dict:
            if col and col in state.df_dict[tbl].columns and state.datetime_columns is None:
                updates["datetime_columns"] = col
            elif state.datetime_columns is None:
                df = state.df_dict[tbl]
                date_cols = [c for c in df.columns if any(kw in c.lower() for kw in ["time", "date", "timestamp", "dt"])]
                if date_cols:
                    updates["possible_datetime_columns"] = date_cols

    elif state.task_type == "join_tables":
        for field in ("table_name1", "join_column1", "table_name2", "join_column2"):
            if field in extracted and getattr(state, field) is None:
                updates[field] = extracted[field]

    return updates

