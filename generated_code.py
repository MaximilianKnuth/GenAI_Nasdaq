import pandas as pd
import sys

try:
    # Load datasets
    efr = pd.read_csv('01_Data/EFR.csv')
    eqr = pd.read_csv('01_Data/EQR.csv')
    
    # Perform inner join on 'ticker'
    joined_df = pd.merge(efr, eqr, on='ticker', how='inner')
    
    # Save the result
    joined_df.to_csv('joined_output.csv', index=False)
    
except FileNotFoundError as e:
    print(f"Error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)