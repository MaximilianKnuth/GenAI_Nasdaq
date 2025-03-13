import pandas as pd
from Agents.master_agent import MasterAgent
from docx import Document  # For reading .docx files

## ollama serve
if __name__ == "__main__":
    # Load CSVs into Pandas DataFrames
    file_paths = {
        "EFR": "01_Data/EFR.csv",
        "EQR": "01_Data/EQR.csv",
        "SKMS": "01_Data/SKMS.csv",
        "Schema": "01_Data/Data Product Samples.docx"  # Path to the .docx file
    }

    # Prompt the user to input the DeepSeek API key
    api_key = input("Please enter your DeepSeek API key: ").strip()

    # Load CSV files into DataFrames
    df_dict = {name: pd.read_csv(path) for name, path in file_paths.items() if name != "Schema"}

    # Load the .docx file for schema reference
    schema_doc = Document(file_paths["Schema"])
    schema_text = "\n".join([para.text for para in schema_doc.paragraphs])  # Extract text from .docx

    # Initialize Master Agent with schema reference
    master_agent = MasterAgent(schema_text)

    # Example Queries
    queries = [
        # "Please convert transaction timestamps in the EQR dataset from EST to UTC.",
        # "Please convert transaction timestamps in the EQR dataset from EST.",
        # "Join EFR and EQR based on ticker",
        # "Check the distribution of eqr and sgr"
    ]

    for query in queries: 
        print(f"\nUser Query: {query}")
        result = master_agent.process_query(query, df_dict,api_key)