import pandas as pd
import ollama

class DataTransformationAgent:
    
    def __init__(self,model="deepseek-r1:1.5b"):
        """
        Initializes the Data Transformation Agent using a locally running DeepSeek model via Ollama.
        """
        self.model = model
    
    def clean_deepseek_response(self,response_text):
        """
        Removes any extra text or <think> tags from the DeepSeek response.
        Extracts only the final comma-separated column list.
        """
        # Split response at "</think>" and take the last part
        if "</think>" in response_text:
            response_text = response_text.split("</think>")[-1].strip()

        # Ensure only valid column names are returned
        extracted_columns = [col.strip() for col in response_text.split(",") if col.strip()]
        
        return extracted_columns

    
    def extract_table_and_columns(self, user_query, df_dict):
        """
        Uses DeepSeek LLM to determine the relevant table and column(s) from the user query.
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
        **Return ONLY the column names as a comma-separated list, nothing else.**

        ### OUTPUT FORMAT:
        Example: `date, timestamp, latestquarter`
        """

        # Run DeepSeek locally using Ollama
        try:
            response = ollama.chat(model=self.model, messages=[{"role": "user", "content": prompt}],options={"temperature": 0})
            raw_output = response["message"]["content"].strip()

            # Clean the response to remove <think> and extract columns
            selected_columns = self.clean_deepseek_response(raw_output)
        
            # Ensure only valid columns are returned
            selected_columns = [col for col in selected_columns if col in df.columns]

            if not selected_columns:
                print(f"No datetime columns identified by DeepSeek in {table_name}.")
                return table_name, None

            return table_name, selected_columns

        except Exception as e:
            print(f"Error in DeepSeek column extraction: {e}")
            return table_name, None
    
    def execute(self, user_query, df_dict):
        """
        Extracts required information using DeepSeek and converts datetime columns to UTC.
        """
        table_name, datetime_columns = self.extract_table_and_columns(user_query, df_dict)

        if not table_name or not datetime_columns:
            print("Missing required information. Task cannot be completed.")
            return None

        df = df_dict[table_name]

        for col in datetime_columns:
            try:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize("UTC")
                print(f"Converted {col} to UTC in {table_name}")
            except Exception as e:
                print(f"Error converting {col}: {e}")

        return df