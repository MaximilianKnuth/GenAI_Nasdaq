import pandas as pd
import re
import pytz
from transformers import pipeline
from openai import OpenAI
from Agents.chunk_table import trunk_table_execute

class TimezoneExtractor:
    def __init__(self, api_key):
        """
        Initializes the TimezoneExtractor with the DeepSeek model.
        """
        self.model = "deepseek-chat"  # Use the correct model name
        self.client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")

    def extract_timezones(self, user_query):
        """
        Extracts the original and target timezones the user asks for from the user query.
        Returns the timezones in pytz format as [transformed_original, transformed_target].
        If either timezone is not found, returns None for that timezone.
        """
        all_timezones = pytz.all_timezones

        # Define the prompt for extracting both timezones
        prompt = f"""
        You are an AI assistant to find the corresponding timezones in **{user_query}** to their correspondence in **"{all_timezones}"**.

        ### TASK:
        1. Extract the **original timezone** (the timezone the data is currently in).
        2. Extract the **target timezone** (the timezone the user wants to convert to).
        3. If either timezone is not explicitly mentioned, return `None` for that timezone.

        ### IMPORTANT RULES:
        - Output ONLY the corresponding timezones in the format `Region/City` (e.g., `US/Eastern` or `UTC`), nothing more.
        - If a timezone is not found, return `None` for that timezone.
        - Do not include any additional text or explanations.

        ### OUTPUT FORMAT:
        Example: `US/Eastern, UTC`
        """

        try:
            # Query the DeepSeek API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt},
                ],
                stream=False,
                temperature=0  # Set temperature to 0 for deterministic output
            )

            # Extract the raw output from the API response
            raw_output = response.choices[0].message.content.strip()

            # Split the output into original and target timezones
            timezones = raw_output.split(", ")
            if len(timezones) != 2:
                raise ValueError("Invalid number of timezones returned.")

            # Extract and validate timezones
            original_timezone = pytz.timezone(timezones[0]) if timezones[0].lower() != "none" else None
            target_timezone = pytz.timezone(timezones[1]) if timezones[1].lower() != "none" else None

            if original_timezone==None:
                original_timezone = input("Please enter the original timezone when the table is built in pytz library format (if it's local, then enter 'UTC'): ").strip()
            if target_timezone==None:
                target_timezone = input("Please enter the target timezone you want to convert to in pytz library format (if it's local, then enter 'UTC'): ").strip()
            return [original_timezone, target_timezone]

        except Exception as e:
            print(f"Error extracting timezones: {e}")
            return [None, None]  # Return None for both timezones if extraction fails


class DataTransformationAgent:
    def __init__(self, model=None):
        """
        Uses the `llama3` model via Ollama.
        """
        # self.model = model

    def extract_table_and_columns(self, user_query, df_dict):
        
        # extract table name from query
        possible_tables = df_dict.keys()
        table_name = next((table for table in possible_tables if table.lower() in user_query.lower()), None)

        if not table_name:
            print("No valid table found in query.")
            return None, None

        # extract date column from table
        try:
            summary, table_data = trunk_table_execute(table_name)
            date_columns=table_data['Date']
            return table_name, date_columns

        except Exception as e:
            print(f"No Column in table {table_name} is declared as Date type: {e}")
            return table_name, None

    def execute(self, user_query, df_dict, api_key):
        """
        Converts datetime columns based on inferred timezones and provides a clean output.
        """
        # Extracts relevant table and time-related columns
        table_name, datetime_columns = self.extract_table_and_columns(user_query, df_dict)

        if not table_name or not datetime_columns:
            print("Missing required information. Task cannot be completed.")
            return None

        # df = df_dict[table_name]

        # Extract original and target timezones from the user query
        timezone_extractor = TimezoneExtractor(api_key)
        original_timezone, target_timezone = timezone_extractor.extract_timezones(user_query)

        output = {
            "columns_converted": [],
            "original_timezone": original_timezone,
            "new_timezone": target_timezone
        }

        # for col in datetime_columns:
        #     try:
        #         df[col] = pd.to_datetime(df[col]).dt.tz_localize(original_timezone).dt.tz_convert(target_timezone)
        #         output["columns_converted"].append(col)
        #         print(f"Converted {col} from {original_timezone} to {target_timezone} in {table_name}")
        #     except Exception as e:
        #         print(f"Error converting {col}: {e}")

        print("\n### Conversion Summary ###")
        print(f"0. Executed Table: {table_name}")
        print(f"1. Columns being converted: {datetime_columns}")
        print(f"2. Original timezone: {output['original_timezone']}")
        print(f"3. New timezone: {output['new_timezone']}")

        return output