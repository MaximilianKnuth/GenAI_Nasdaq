import pandas as pd
import re
import pytz
from transformers import pipeline
from openai import OpenAI
from Agents.chunk_table import trunk_table_execute
from Agents.date_validation_agent import DateValidationAgent
from Agents.retrieval_agent import RAG_retrieval

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
        #all_timezones = pytz.all_timezones
        #print(all_timezones)

        # Define the prompt for extracting both timezones
        prompt = f"""
        You are an AI assistant to find the current timezones and the target timezone in: **{user_query}**.

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
        Example: `None, UTC`
        Example: `UTC, None`
        Example: `None, None`
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
            #print(f"Raw Output from Deepseek: {raw_output}")
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
            original_timezone = input("Please enter the original timezone when the table is built in pytz library format (if it's local, then enter 'UTC'): ").strip()
            target_timezone = input("Please enter the target timezone you want to convert to in pytz library format (if it's local, then enter 'UTC'): ").strip()
            return [original_timezone, target_timezone]  # Return None for both timezones if extraction fails


class DataTransformationAgent:
    def __init__(self, model="deepseek-chat", data_validator=None):
        """
        Uses the `llama3` model via Ollama.
        """
        self.model = model
        self.data_validator = data_validator
        self.name = "DataTransformationAgent"
        self.date_validation_agent = DateValidationAgent()
        self.client = None
        
    
    def set_data_validator(self,data_validator):
        self.data_validator = data_validator
        

    def extract_table_and_columns_via_rag(self, user_query, df_dict, api_key):

        # Update these paths/keys as needed
        pdf_folder = "01_Data/text_data"
        openai_api_key = "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"
        deepseek_api_key = "sk-74c415edef3f4a16b1ef8deb3839cf2a"
        
        try:
            # Instantiate the RAG pipeline
            rag = RAG_retrieval(pdf_folder, openai_api_key, deepseek_api_key)
            all_timezones = pytz.all_timezones
            
            prompt = f"""
            Return me the table name and corresponding column name that are of the type datetime the user asked for. If you can't find anything that matches, suggest other table names and column names and explicit name it suggestion.
            **Output Format**
            Exact  Matches: Your answer (if we have exact matches)
            Suggestion: If we dont have exact matches, then return the table name and datetime column names that are similar to the task.
            """
        
            prompt = prompt + ": " + user_query
            answer = rag.test_pipeline(prompt)
            print(f"Answer from RAG: {answer}")
        except Exception as e:
            print(f"Error extracting timezones: {e}")
        # Define the parsing prompt for extracting table name and datetime columns
        
        
        parsing_prompt = f"""
        You are a helpful AI assistant to find the corresponding table name and column name based on the user query: {user_query}, and based on the returned information from the RAG that contains tables and column names: {answer}.

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

        try:
            # Query the DeepSeek API
            self.client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
            #print(f"Parsing prompt for Deepseek: {parsing_prompt}")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant and only return either the the table and column names or 'None' ."},
                    {"role": "user", "content": parsing_prompt},
                ],
                stream=False,
                temperature=0  # Set temperature to 0 for deterministic output
            )
            

            # Extract the raw output from the API response
            raw_output = response.choices[0].message.content.strip()
            
            #print(f"Raw Output from Deepseek: {raw_output}")
            
            if raw_output.lower() == "none":
                return [None, None, answer]

            # Split the output
            output_text = raw_output.split(", ")
            #print(output_text)
            if len(output_text) != 2:
                raise ValueError(answer)
            else:
                table_name = output_text[0]
                datetime_columns = output_text[1]

        except Exception as e:
            #print(f"Error extracting timezones: {e}")
            return [None, None, answer]  # Return None for both timezones if extraction fails

        return table_name, datetime_columns, answer
    
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
        #print("Execute")
        """
        Converts datetime columns based on inferred timezones and provides a clean output.
        """
        # Extracts relevant table and time-related columns
        #table_name, datetime_columns = self.extract_table_and_columns(user_query, df_dict)

        # Attempt to extract table and columns via RAG
        table_name, datetime_columns, detailed_answer = self.extract_table_and_columns_via_rag(
            user_query=user_query,
            df_dict=df_dict,
            api_key=api_key
        )
        #print(table_name)
        #print(datetime_columns)
        #print(f"detailed answer: {detailed_answer}")
        
        if  table_name is None or datetime_columns is None:
            
            table_name=None
            datetime_columns=None
            print(detailed_answer)
            print("")
            print("Please provide some clarification:")
            print("USER INPUT:")
            user_answer = input("").strip()
            new_user_query = detailed_answer + " The user said: "+ user_answer
            
            return self.execute(new_user_query,df_dict, api_key)
            
            

        # df = df_dict[table_name]

        # Extract original and target timezones from the user query
        timezone_extractor = TimezoneExtractor(api_key)
        original_timezone, target_timezone = timezone_extractor.extract_timezones(detailed_answer)

        validation_summary = self.data_validator.validate_dataframe(df_dict[table_name])
        df = df_dict[table_name]
        print("\n### Date Column Validation ###")
        
        try:
            result = self.date_validation_agent.validate_date_column(df, datetime_columns)
            if result["valid"]:
                print(f"[{datetime_columns}] VALID → {result['message']}")
            else:
                print(f"[{datetime_columns}] INVALID → {result['message']}")
                print(f"    Recommendation: {result['recommendation']}")
        except Exception as e:
            print(f"[{datetime_columns}] DateValidationAgent failed: {str(e)}")
                
        #result = self.date_validation_agent.validate_date_column(table_name, datetime_columns)
        
        output = {
            "Executed Table": table_name,
            "columns_converted": datetime_columns,
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
        
        print("\n### Data Validation Summary ###")
        print(f"0. Quality check passed: {validation_summary['valid']}")
        print(f"1. Errors: {validation_summary['errors']}")
        print(f"2. Warnings: {validation_summary['warnings']}")
        print(f"3. Quality metrics: {validation_summary['quality_metrics']}")

        print("\n### Conversion Summary ###")
        print(f"0. Executed Table: {table_name}")
        print(f"1. Columns being converted: {datetime_columns}")
        print(f"2. Original timezone: {output['original_timezone']}")
        print(f"3. New timezone: {output['new_timezone']}")

        return output