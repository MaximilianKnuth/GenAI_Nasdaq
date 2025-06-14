import pandas as pd
from Agents.master_agent import MasterAgent
from docx import Document  # For reading .docx files

## ollama serve
if __name__ == "__main__":
    # Load CSVs into Pandas DataFrames
    file_paths = {
        "EFR": "Data/EFR.csv",
        "EQR": "Data/EQR.csv",
        "SKMS": "Data/SKMS.csv",
    }

    # Prompt the user to input the DeepSeek API key
    api_key = 'sk-74c415edef3f4a16b1ef8deb3839cf2a' #input("Please enter your DeepSeek API key: ").strip()

    # Load CSV files into DataFrames
    df_dict = {name: pd.read_csv(path) for name, path in file_paths.items()}

    #sk-74c415edef3f4a16b1ef8deb3839cf2a
    
    # Example Queries
    new_queries = [
        "Please convert transaction timestamps in the SKMS dataset from ET timzeone to UTC timezone.",
        #Please convert the datetime column in the EQR dataset from EST timezone to UTC timezone.",
        
    ]
    

    # Example Queries
    queries = [
        # "Please convert transaction timestamps in the EQR dataset from EST to UTC.",
        # "Please convert transaction timestamps in the SKMS dataset from EST.",
        # "Join EFR and EQR based on ticker",
        # "Check the distribution of eqr and sgr"
    ]

    for query in new_queries: 
        print(f"\nUser Query: {query}")
        master_agent = MasterAgent(query, df_dict,api_key)
        master_agent.execute_task()