import pandas as pd
import pytz
from datetime import datetime
import sys

try:
    # Load the dataset
    df = pd.read_csv('01_Data/SKMS.csv')
    
    # Convert New_date from EST to UTC
    eastern = pytz.timezone('US/Eastern')
    df['New_date'] = pd.to_datetime(df['New_date'])
    df['New_date'] = df['New_date'].apply(lambda x: eastern.localize(x).astimezone(pytz.utc))
    
    # Save transformed dataset
    df.to_csv('SKMS_transformed.csv', index=False)
    
except FileNotFoundError:
    print("Error: File '01_Data/SKMS.csv' not found.")
    sys.exit(1)
except Exception as e:
    print(f"Error: {str(e)}")
    sys.exit(1)