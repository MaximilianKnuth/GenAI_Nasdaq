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
import traceback

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
    # tz_1 = _canonical_tz(pieces[0]) if len(pieces) > 0 else None
    # tz_2 = _canonical_tz(pieces[1]) if len(pieces) > 1 else None

    # _TZ_CACHE[user_query] = (tz_1, tz_2)
    return pieces[0], pieces[1]

# Task Classification Agent
def task_classification_agent(state: AgentState) -> Dict[str, Any]:
    print("task_classification_agent")
    """Classify the tasks based on user query"""
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")

    while True:
        # Define the multi-task classification prompt
        prompt = f"""
        The following are possible task categories:
        - convert_datetime: When the user wants to convert datetime/timestamps between timezones
        - join_tables: When the user wants to join two or more tables together
        - check_distribution: When the user wants to analyze data distribution

        Given the user query: "{state.user_query}"

        Identify ALL relevant tasks mentioned in the query.  
        Return your answer as a Python list of category names, like this:
        ["convert_datetime", "join_tables"]

        Do not include anything else other than the Python list.
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

        # Parse LLM output
        task_list_str = response.choices[0].message.content.strip()

        # Convert string to actual list
        import ast
        try:
            task_list = ast.literal_eval(task_list_str)
        except Exception as e:
            print(f"Failed to parse task list: {task_list_str}")
            raise e

        # Show the detected tasks to the human
        print(f"Detected the following tasks, we will execute it sequentially: {task_list}. \n Additional Info on what each task do: convert_datetime:\n ➔ Convert datetime or timestamps between different timezones or formats.\n join_tables:\n ➔ Combine two or more tables together based on matching key columns.")
        confirmation = input("Do you want to proceed with these tasks? (yes/no): ")

        if confirmation.strip().lower() == "yes":
            # If confirmed, update the state and break the loop
            return {"task_list": task_list, "task_length": len(task_list),"task_step":1,"user_query": state.user_query}
        else:
            # Otherwise, ask the user to input a new query
            new_query = input("Please enter a more precise query: ")
            state.user_query = new_query
            print("Retrying classification with updated query...")
            return task_classification_agent(state)

def split_query_agent(state: AgentState) -> Dict[str, Any]:
    print("split query")
    """Call LLM to generate rewritten queries specific to each task."""
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
    rewritten_queries = []

    prompt = f"""
    You are a helpful assistant.

    The user originally asked: "{state.user_query}"

    Now, the task we want to focus on is: "{state.task_list}.\nAdditional Info on what each task do: convert_datetime:\n ➔ Convert datetime or timestamps between different timezones or formats.\n join_tables:\n ➔ Combine two or more tables together based on matching key columns."

    For each task listed above, generate a specific, detailed, rewritten user query focusing ONLY on that task.

    Return your output as a Python list of rewritten queries, one for each task, in the same order.
    Example output format:
    ["Rewritten query for task 1", "Rewritten query for task 2", ...]

    DO NOT include any explanation, commentary, or additional text. Only return the Python list.
    """

    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "user", "content": prompt},
        ],
        temperature=0
    )

    rewritten_queries_str = response.choices[0].message.content.strip()

    # Parse the list from string
    import ast
    try:
        rewritten_queries = ast.literal_eval(rewritten_queries_str)
    except Exception as e:
        print(f"Failed to parse rewritten queries: {rewritten_queries_str}")
        raise e
    print("new queries:", rewritten_queries)
    return {"task_query":rewritten_queries}


# RAG Agent for table and column extraction if not defined - timezone task
def tz_rag_agent(state: AgentState) -> Dict[str, Any]:
    print("rag_agent")
    """Enhanced RAG agent that incorporates human input when available"""
    pdf_folder = "01_Data/text_data"
    
    try:
        # Instantiate the RAG pipeline
        rag = RAG_retrieval(pdf_folder, state.openai_api_key, state.api_key)
        current_user_query=state.task_query[state.task_step-1]

        # Build enhanced prompt incorporating human input if available
        combined_query = current_user_query
        if state.human_response:
            combined_query = f"""
            Original query: {current_user_query}
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
        
        Original query: {current_user_query}
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
        output_table_name = output_text[0]
        output_datetime_column = output_text[1]
        if len(output_text) != 2:
            return {
                "needs_human_input": True,
                "human_message": f"I couldn't parse the table and column information. {answer}\n\nPlease specify which table and column to use:",
                "rag_results": answer,
                "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
                "human_response": None  # Clear human response since we've used it
            }
        elif (state.table_name and state.table_name != output_table_name) or (state.datetime_columns and state.table_name != output_datetime_column):    #rag returns differently than user specified
            return {
                "needs_human_input": True,
                "rag_results": answer,
                "historical_response": (state.historical_response or "") + "\n" + (state.human_response or ""),
                "human_response": None,  # Clear human response since we've used it
                "human_message" : f"I couldn't find the matching column and table. Please make sure you specify the correct table and column. \n Suggestion: {answer}:"
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
    
    current_user_query=state.task_query[state.task_step-1]
    suggestion = rag.test_pipeline(current_user_query)
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
    current_user_query=state.task_query[state.task_step-1]
    combined_query = current_user_query
    
    # Incorporate human input if available
    if state.human_response:
        combined_query = f"{current_user_query}\nAdditional information: {state.human_response} {state.historical_response}"
        # Clear human response as we're using it
        updates["human_response"] = None
    
    # # --- 1. Heuristic scan of the combined text ------------------------------
    # for token in combined_query.replace(",", " ").split():
    #     tz = _canonical_tz(token)
    #     if tz:
    #         if not state.original_timezone:
    #             updates["original_timezone"] = tz
    #         elif not state.target_timezone:
    #             updates["target_timezone"] = tz
    #     if updates.get("original_timezone") and updates.get("target_timezone"):
    #         break

    # --- 2. If still missing anything, call the LLM with combined context ----------------------
    # if not (
    #     (state.original_timezone or updates.get("original_timezone"))
    #     and (state.target_timezone or updates.get("target_timezone"))
    # ):
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

    # --- 3. If incomplete, defer to human --------------------------
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
        # print("oops")
        df = state.df_dict[state.table_name]
        
        # Check if column exists
        if not state.datetime_columns:
            missing.append("datetime column")
        elif state.datetime_columns not in df.columns:
            # print("oops")
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
    # print(state.rag_results)
    result = ""
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")
    pdf_folder = "01_Data/text_data"

    task=state.task_list[state.task_step-1]
    current_user_query=state.task_query[state.task_step-1]
    # print(current_user_query)
    if task == "convert_datetime":
        # --- TABLE SUGGESTIONS ---
        if "table name" in missing or any("table" in err for err in invalid):
            # First use available tables from df_dict
            if "available_tables" in suggestions:
                result += f"📊 Available tables: {', '.join(suggestions['available_tables'])}\n\n"
            
            # Then augment with RAG if needed
            try:
                pdf_folder = "01_Data/text_data"
                rag = RAG_retrieval(pdf_folder, state.openai_api_key, state.api_key)
                
                rag_query = f"What tables would be most relevant for this query: {current_user_query}?"
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
            Based on this user query: "{current_user_query}"
            
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
        

    elif task == "join_tables":
        for key in ["table_name1", "table_name2"]:
            if key in missing or any(key.split("_")[1] in err for err in invalid):
                if suggestions.get(f"{key}_suggestions"):
                    result += f"📊 Suggested {key}: {', '.join(suggestions[f'{key}_suggestions'])}\\n\\n"

        for key in ["join_column1", "join_column2"]:
            if key in missing or any(key.split("_")[1] in err for err in invalid):
                parent = "table_name1" if "1" in key else "table_name2"
                table = getattr(state, parent, None)
                if table and table in state.df_dict:
                    df = state.df_dict[table]
                    result += f"🔗 Columns in {table}: {', '.join(df.columns[:10])}\\n\\n"

        try:
            rag = RAG_retrieval(pdf_folder, state.openai_api_key, state.api_key)
            rag_output = rag.test_pipeline(f"Suggest compatible table pairs and join keys for this query: {current_user_query}")
            result += f"💡 RAG-based suggestion: {rag_output}\\n\\n"
        except Exception as e:
            logging.warning(f"RAG fallback join suggestion error: {str(e)}")

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

# Validation Agent - useless currently
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


def code_generation_agent(state: AgentState) -> Dict[str, Any]:
    """Generate code based on task type and execution context"""
    print("code_generation_agent")
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")

    if state.task_length==1:#check if is single task
        if state.task_type == "convert_datetime":
            file_path = f"01_Data/{state.table_name}.csv"
            try:
                df = pd.read_csv(file_path, nrows=5)
                sample_data = df.to_markdown(index=False)
            except Exception as e:
                sample_data = f"[Error reading {file_path}: {str(e)}]"

            prompt = f"""
            You are a Python code generation assistant. The user provided:

            QUERY: "{state.user_query}"

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
            print(f'''
            EXECUTION CONTEXT:
            - Table: {state.table_name}
            - Datetime columns to convert: {state.datetime_columns}
            - Original timezone: {state.original_timezone}
            - Target timezone: {state.target_timezone}
            ''')
            
        elif state.task_type == "join_tables":
            file_path1 = f"01_Data/{state.table_name1}.csv"
            file_path2 = f"01_Data/{state.table_name2}.csv"
            try:
                df1 = pd.read_csv(file_path1)
                df2 = pd.read_csv(file_path2)
                sample_data = (
                    f"Table 1: {state.table_name1}\n"
                    f"{df1.to_markdown(index=False)}\n\n"
                    f"Table 2: {state.table_name2}\n"
                    f"{df2.to_markdown(index=False)}"
                )
            except Exception as e:
                sample_data = f"[Error reading input files: {str(e)}]"

            prompt = f"""
            You are a Python code generation assistant. The user provided:

            QUERY: "{state.user_query}"

            EXECUTION CONTEXT:
            - Table 1: {state.table_name1}
            - Join Column for table 1: {state.join_column1}
            - Table 2: {state.table_name2}
            - Join Column for table 2: {state.join_column2}

            ### Dataset Preview (first 5 rows from each table):
            {sample_data}

            TASK REQUIREMENTS:
            1. Load both datasets from '{file_path1}' and '{file_path2}'
            2. Perform an inner join on {state.join_column1} from table 1 and {state.join_column2} from table 2
            3. Save the result as 'joined_output.csv'
            4. Return ONLY the executable Python code without any Markdown formatting
            5. Include proper error handling
            6. Do not use ```python or ``` markers
            """

            print(f'''
            EXECUTION CONTEXT:
            - Table 1: {state.table_name1}
            - Join Column for table 1: {state.join_column1}
            - Table 2: {state.table_name2}
            - Join Column for table 2: {state.join_column2}
            ''')
            
        else:
            return {"error": "Unsupported task type for code generation."}

        response = client.chat.completions.create(
            model="deepseek-coder",
            messages=[
                {"role": "system", "content": "You are a helpful Python coding assistant."},
                {"role": "user", "content": prompt},
            ],
            temperature=0
        )

    else:
        # Build execution context for all tasks
        execution_context = []
        dataset_previews = []

        for idx, task in enumerate(state.task_list):
            task_step = idx + 1

            if task == "convert_datetime":
                file_path = f"01_Data/{state.table_name}.csv"
                try:
                    df = pd.read_csv(file_path, nrows=5)
                    sample_data = df.to_markdown(index=False)
                except Exception as e:
                    sample_data = f"[Error reading {file_path}: {str(e)}]"

                execution_context.append(f"""
                Step {task_step}: Convert Datetime
                - Table: {state.table_name}
                - Datetime columns to convert: {state.datetime_columns}
                - Original timezone: {state.original_timezone}
                - Target timezone: {state.target_timezone}
                """)
                dataset_previews.append(f"Step {task_step} Dataset Preview:\n{sample_data}")

            elif task == "join_tables":
                file_path1 = f"01_Data/{state.table_name1}.csv"
                file_path2 = f"01_Data/{state.table_name2}.csv"
                try:
                    df1 = pd.read_csv(file_path1, nrows=5)
                    df2 = pd.read_csv(file_path2, nrows=5)
                    sample_data = (
                        f"Table 1: {state.table_name1}\n"
                        f"{df1.to_markdown(index=False)}\n\n"
                        f"Table 2: {state.table_name2}\n"
                        f"{df2.to_markdown(index=False)}"
                    )
                except Exception as e:
                    sample_data = f"[Error reading input files: {str(e)}]"

                execution_context.append(f"""
                Step {task_step}: Join Tables
                - Table 1: {state.table_name1}
                - Join Column for table 1: {state.join_column1}
                - Table 2: {state.table_name2}
                - Join Column for table 2: {state.join_column2}
                """)
                dataset_previews.append(f"Step {task_step} Dataset Preview:\n{sample_data}")

            else:
                return {"error": f"Unsupported task type '{task}' for multi-task generation."}

        # Combine execution context
        full_execution_context = "\n".join(execution_context)
        full_dataset_preview = "\n\n".join(dataset_previews)

        # Build the prompt
        prompt = f"""
        You are a Python code generation assistant. The user provided:

        QUERY: "{state.user_query}"

        EXECUTION SEQUENCE:
        {', '.join(state.task_list)}

        EXECUTION CONTEXT:
        {full_execution_context}

        ### Dataset Previews (first 5 rows from each relevant table):
        {full_dataset_preview}

        TASK REQUIREMENTS:
        1. Execute each task sequentially as listed above.
        2. Make sure the output of each step is properly handled for the next step.
        3. Load datasets from the specified file paths.
        4. For datetime conversion, transform columns from {state.original_timezone} to {state.target_timezone}.
        5. For joining tables, perform an inner join on specified columns.
        6. Save intermediate results appropriately (e.g., after datetime conversion, save a temp file if needed).
        7. The final output should be saved with a clear filename (e.g., 'final_output.csv').
        8. Return ONLY the full executable Python code without any Markdown formatting.
        9. Include proper error handling at each step.
        10. Do not use ```python or ``` markers.
        """

        print("Multi-Task Execution Context:")
        print(full_execution_context)

        # Call the LLM
        response = client.chat.completions.create(
            model="deepseek-coder",
            messages=[
                {"role": "system", "content": "You are a helpful Python coding assistant."},
                {"role": "user", "content": prompt},
            ],
            temperature=0
        )

        
    code = response.choices[0].message.content.strip()
    if code.startswith('```python') and code.endswith('```'):
        code = code[9:-3].strip()
    elif code.startswith('```') and code.endswith('```'):
        code = code[3:-3].strip()

    with open("generated_code.py", "w") as f:
        f.write(code)

    return {"generated_code": code}

# Human Input Handler, update field based on human input, if input is invalid, find closest and ask for confirmation
def process_human_input(state: AgentState) -> Dict[str, Any]:
    print("process_human_input")

    reply = (state.human_response or "").strip()
    if not reply:
        return {}

    updates: Dict[str, Any] = {}
    updates["historical_response"]= (state.historical_response or "") + "\n" + (state.human_response or "")
    
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")

    task=state.task_list[state.task_step-1]
    
    # Task-specific prompt & expected keys
    if task == "convert_datetime":
        prompt = f"""
        Extract table name, datetime column, original timezone, and target timezone from the user's clarification.

        User query: "{state.user_query} {state.historical_response}"
        User response: "{reply}"

        Return a JSON object:
        {{
            "table_name": str or null,
            "datetime_columns": str or null,
            "original_timezone": str or null,
            "target_timezone": str or null
        }}
        """
        expected_keys = ["table_name", "datetime_columns", "original_timezone", "target_timezone"]

    elif task == "join_tables":
        prompt = f"""
        Extract the two table names and their join columns from the user's clarification.

        User query: "{state.user_query} {state.historical_response}"
        User response: "{reply}"

        Return a JSON object:
        {{
            "table_name1": str or null,
            "join_column1": str or null,
            "table_name2": str or null,
            "join_column2": str or null
        }}
        """
        expected_keys = ["table_name1", "join_column1", "table_name2", "join_column2"]
    else:
        return {}

    # LLM-based extraction
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

        import re
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            extracted = json.loads(json_match.group(0))
            for k in expected_keys:
                v = extracted.get(k)
                if v is not None:
                    if "timezone" in k:
                        v = _canonical_tz(v) or v
                    updates[k] = v

    except Exception as e:
        logging.warning(f"LLM parsing error: {str(e)}")

    suggestions = []
    # Post-processing
    if task == "convert_datetime":
        if "table_name" in updates and updates["table_name"] not in state.df_dict:
            closest = min(state.df_dict.keys(), key=lambda t: abs(len(t) - len(updates["table_name"])), default=None)
            if closest:
                suggestions.append(f"❓ Table '{updates['table_name']}' not found. Did you mean '{closest}'?")
                # Do not auto-correct
                updates["table_name"] = None

    elif task == "join_tables":
        for table_key, col_key in [("table_name1", "join_column1"), ("table_name2", "join_column2")]:
            table = updates.get(table_key)
            column = updates.get(col_key)

            # Suggest correction for table
            if table and table not in state.df_dict:
                closest = min(state.df_dict.keys(), key=lambda t: abs(len(t) - len(table)), default=None)
                if closest:
                    suggestions.append(f"❓ Table '{table}' not found. Did you mean '{closest}'?")
                    updates[table_key] = None

            # Suggest correction for column
            if table and column and table in state.df_dict:
                table_columns = state.df_dict[table].columns
                if column not in table_columns:
                    closest_col = min(
                        table_columns,
                        key=lambda c: abs(len(c) - len(column)) + (
                            0 if column.lower() in c.lower() or c.lower() in column.lower() else 5),
                        default=None
                    )
                    if closest_col:
                        suggestions.append(f"❓ Column '{column}' not found in '{table}'. Did you mean '{closest_col}'?")
                        updates[col_key] = None

    updates.update({
        "first_run": False,
    })

    # Final output: if any suggestions, ask user for confirmation
    if suggestions:
        updates["needs_human_input"] = True
        updates["human_message"] = "\n".join(suggestions) + "\n\nPlease confirm or correct the values."
    else:
        updates.update({
            "needs_human_input": False,
            "human_message": None,
        })

    #update user and sub-queries
    result = update_query(state)
    updates.update({
            "user_query": result["full_query"],
            "task_query": result["split_queries"],
        })
    
    return updates

def update_query(state):
    """Use LLM to revise the original query based on user feedback for a specific step."""
    client = OpenAI(api_key=state.api_key, base_url="https://api.deepseek.com")

    task_to_update = state.task_list[state.task_step - 1]  # current task (index correction: step is 1-based)

    prompt = f"""
    You are a helpful assistant.

    The original user query was:
    "{state.user_query}"

    The user had multiple tasks to accomplish under this sequence:
    {', '.join(state.task_list)}

    Now, the user wants to revise the step related to "{task_to_update}" based on the following updated instruction:
    "{state.human_response}"

    Your tasks are:
    1. Revise the full original user query to reflect this updated requirement.
    2. Split the new full query into multiple task-specific queries, one for each task in the same original order.

    Return ONLY a valid Python dictionary with two keys:
    - "full_query": the revised full user query
    - "split_queries": a list of new split queries corresponding to each task

    ### Strict Output Format:
    {{"full_query": "new full query here", "split_queries": ["split query for task 1", "split query for task 2", ...]}}

    Do NOT add any explanation, markdown formatting, or commentary.
    Only return the dictionary directly.
    """

    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "user", "content": prompt},
        ],
        temperature=0
    )

    reply_text = response.choices[0].message.content.strip()

    # 🛠️ Fix: Remove triple backticks if they exist
    if reply_text.startswith("```") and reply_text.endswith("```"):
      reply_text = reply_text.strip("```").strip()
    if reply_text.startswith("python"):
      reply_text = reply_text[len("python"):].strip()
      
    import ast
    try:
        result = ast.literal_eval(reply_text)
    except Exception as e:
        print(f"Failed to parse updated query output: {reply_text}")
        raise e

    return result

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
    
    task=state.task_list[state.task_step-1]
#if it's convert datetime task
    if task == "convert_datetime":
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
        Example: 'Convert timestamps in TableX from EST to UTC'
        Output: {{
            "table_name": "TableX",
            "datetime_column": "timestamps",
            "original_timezone": "US/Eastern",
            "target_timezone": "UTC"
        }}
        """


# if it's join_table task
    elif task == "join_tables":
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
        Use null for unknown that are not specified in query.
        
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

    if task == "convert_datetime":
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

    elif task == "join_tables":
        for field in ("table_name1", "join_column1", "table_name2", "join_column2"):
            if field in extracted and getattr(state, field) is None:
                updates[field] = extracted[field]

    return updates

