import pandas as pd
import ollama
import re
import pytz

class DataTransformationAgent:
    def __init__(self, schema_text=None, model="llama3"):
        """
        Initializes the Data Transformation Agent with schema context.
        Uses the `llama3` model via Ollama.
        """
        self.schema_text = schema_text
        self.model = model

    def clean_response(self, response_text):
        """
        Cleans the response from Ollama to extract relevant information.
        Removes any extra text or unwanted artifacts.
        """
        # # Remove unwanted artifacts (e.g., <think> tags, explanations)
        # response_text = response_text.strip()
        # response_text = re.sub(r"<think>.*?</think>", "", response_text, flags=re.DOTALL)
        # response_text = re.sub(r"Explanation:.*", "", response_text, flags=re.DOTALL)

        # Extract only the final comma-separated list
        extracted_items = [item.strip() for item in response_text.split(",") if item.strip()]
        return extracted_items

    def extract_timezones_names(text):
        """
        Extracts timezones in the format `XXX,XXX` from a large text.
        """
        # Regex pattern to match two valid timezone strings separated by a comma
        timezone_pattern = r"\b([A-Za-z_]+/[A-Za-z_]+),\s*([A-Za-z_]+/[A-Za-z_]+)\b"

        # Search for the pattern in the text
        match = re.search(timezone_pattern, text)

        if match:
            # Extract the two timezones
            timezone1, timezone2 = match.groups()
            return timezone1, timezone2
        else:
            return None, None
    
    def extract_timezones(self, user_query):
        all_timezones = pytz.all_timezones
        
        """
        Extracts the original and target timezones from the user query using Ollama 3.
        """
        prompt = f"""

        ### TASK:
        1. The user provided this input: **"{user_query}"**  
        2. Identify what is the original timezone and the new target timezone from the user query.
        3. Transform the identified original timezone and the target timezone to its correspondance timezone in **"{all_timezones}"**
        4. Return ONLY the two transformed timezones as a comma-separated list as (transformed original timezone, transformed target timezone).

        ### IMPORTANT RULES:
        1. If the original timezone is not explicitly mentioned, assume it is **UTC**.
        2. If the target timezone is not explicitly mentioned, assume it is **UTC**.
        3. Do not include anything else not even reasoning in the output, just two timezones.
        
        ### OUTPUT FORMAT:
        Example: `US/Eastern, UTC`
        """

        try:
            # Query Ollama 3
            response = ollama.chat(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": 0}
            )
            raw_output = response["message"]["content"].strip()

            
            print("timezone output:", raw_output)
            
            # Clean the response
            timezones = self.clean_response(self.extract_timezones_names(raw_output))

            # Extract original and target timezones
            original_timezone = timezones[0] if len(timezones) > 0 else "UTC"
            target_timezone = timezones[1] if len(timezones) > 1 else "UTC"

            return original_timezone, target_timezone

        except Exception as e:
            print(f"Error extracting timezones: {e}")
            return "UTC", "UTC"  # Default to UTC if extraction fails

    def extract_table_and_columns(self, user_query, df_dict):
        """
        Uses Ollama 3 to determine the relevant table and column(s) from the user query.
        """
        possible_tables = df_dict.keys()
        table_name = next((table for table in possible_tables if table.lower() in user_query.lower()), None)

        if not table_name:
            print("No valid table found in query.")
            return None, None

        # Fetch available columns from the identified table
        df = df_dict[table_name]
        available_columns = ", ".join(df.columns)

        # Define LLM Prompt
        prompt = f"""
        You are an AI assistant that extracts column names related to datetime from a dataset.

        ### TASK:
        A user wants to convert datetime columns to UTC.  
        The user provided this input: **"{user_query}"**  
        The table `{table_name}` contains the following columns: **{available_columns}**.

        ### IMPORTANT RULES:
        If the user explicitly mentions a column name, **prioritize that column** (if it exists in the table).  
        If the user **does not specify** a column, **identify relevant datetime-related columns automatically**.  
        **ONLY return valid column names related to date, time, or timestamps**.  
        **IGNORE text, IDs, tickers, financial values, and irrelevant fields**.  
        **Return ONLY the identified table column names as a comma-separated list, nothing else.**

        ### OUTPUT FORMAT, do not include anything else including reasoning:
        Example: `date, timestamp, latestquarter`
        """

        try:
            # Query Ollama 3
            response = ollama.chat(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": 0}
            )
            raw_output = response["message"]["content"].strip()

            # print("column output:", raw_output)
            
            # Clean the response
            selected_columns = self.clean_response(raw_output)

            # Ensure only valid columns are returned
            selected_columns = [col for col in selected_columns if col in df.columns]

            if not selected_columns:
                print(f"No datetime columns identified in {table_name}.")
                return table_name, None

            return table_name, selected_columns

        except Exception as e:
            print(f"Error in column extraction: {e}")
            return table_name, None

    def execute(self, user_query, df_dict):
        """
        Converts datetime columns based on inferred timezones and provides a clean output.
        """
        # Extracts relevant table and time-related columns
        table_name, datetime_columns = self.extract_table_and_columns(user_query, df_dict)

        if not table_name or not datetime_columns:
            print("Missing required information. Task cannot be completed.")
            return None

        df = df_dict[table_name]

        # Extract original and target timezones from the user query
        original_timezone, target_timezone = self.extract_timezones(user_query)

        output = {
            "columns_converted": [],
            "original_timezone": original_timezone,
            "new_timezone": target_timezone
        }

        for col in datetime_columns:
            try:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(original_timezone).dt.tz_convert(target_timezone)
                output["columns_converted"].append(col)
                print(f"Converted {col} from {original_timezone} to {target_timezone} in {table_name}")
            except Exception as e:
                print(f"Error converting {col}: {e}")

        print("\n### Conversion Summary ###")
        print(f"0. Executed Table: {table_name}")
        print(f"1. Columns being converted: {datetime_columns}")
        print(f"2. Original timezone: {output['original_timezone']}")
        print(f"3. New timezone: {output['new_timezone']}")

        return df