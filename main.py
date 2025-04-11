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
    }

    # Prompt the user to input the DeepSeek API key
    api_key = 'sk-74c415edef3f4a16b1ef8deb3839cf2a' #input("Please enter your DeepSeek API key: ").strip()

    # Load CSV files into DataFrames
    df_dict = {name: pd.read_csv(path) for name, path in file_paths.items()}

    #sk-74c415edef3f4a16b1ef8deb3839cf2a
    

    # Example Queries
    queries = [
        # "Please convert transaction timestamps in the EQR dataset from EST to UTC.",
        "Please convert transaction timestamps in the EQR dataset from EST.",
        # "Join EFR and EQR based on ticker",
        # "Check the distribution of eqr and sgr"
    ]

    for query in queries: 
        print(f"\nUser Query: {query}")
        master_agent = MasterAgent(query, df_dict,api_key)
        master_agent.execute_task()