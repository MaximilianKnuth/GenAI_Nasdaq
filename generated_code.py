import pandas as pd

try:
    # Load datasets
    efr = pd.read_csv('01_Data/EFR.csv')
    eqr = pd.read_csv('01_Data/EQR.csv')
    
    # Perform inner join on 'ticker' column
    joined_df = pd.merge(efr, eqr, on='ticker', how='inner')
    
    # Save the result
    joined_df.to_csv('joined_output.csv', index=False)
    
except FileNotFoundError as e:
    print(f"Error: File not found - {e}")
except pd.errors.EmptyDataError:
    print("Error: One of the files is empty")
except KeyError as e:
    print(f"Error: Missing required column - {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")