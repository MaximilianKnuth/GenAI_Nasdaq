import pandas as pd
import ollama
import re
import pytz
from transformers import pipeline
from openai import OpenAI


class TimezoneExtractor:
    def __init__(self):
        """
        Initializes the TimezoneExtractor with the DeepSeek model.
        """
        self.model = "deepseek-chat"  # Use the correct model name

    def extract_timezones(self, user_query):
        client = OpenAI(api_key="sk-74c415edef3f4a16b1ef8deb3839cf2a", base_url="https://api.deepseek.com")
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
            response = client.chat.completions.create(
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

            return [original_timezone, target_timezone]

        except Exception as e:
            print(f"Error extracting timezones: {e}")
            return [None, None]  # Return None for both timezones if extraction fails
    
    
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
        timezone_extractor=TimezoneExtractor()
        original_timezone, target_timezone = timezone_extractor.extract_timezones(user_query)
        
        #Re

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