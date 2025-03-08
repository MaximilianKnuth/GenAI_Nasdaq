import pandas as pd
from Agents.master_agent import MasterAgent


if __name__ == "__main__":
    # Load CSVs into Pandas DataFrames
    file_paths = {
        "EFR": "01_Data/EFR.csv",
        "EQR": "01_Data/EQR.csv",
        "SKMS": "01_Data/SKMS.csv"
    }

    df_dict = {name: pd.read_csv(path) for name, path in file_paths.items()}

    # Initialize Master Agent
    master_agent = MasterAgent()

    # Example Queries
    queries = [
        "Please ensure that the transaction timestamps in the EQR dataset are converted to UTC."
    #   "Join EFR and EQR based on ticker",
    #    "Check the distribution of eqr and sgr"
    ]

    for query in queries:
        print(f"\nUser Query: {query}")
        result = master_agent.process_query(query, df_dict)
        
        


