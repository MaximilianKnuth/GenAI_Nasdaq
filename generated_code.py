import pandas as pd
from pytz import timezone
import os

def convert_timezone():
    try:
        # Load the dataset
        df = pd.read_csv('01_Data/SKMS.csv')
        
        # Check if New_date column exists
        if 'New_date' not in df.columns:
            raise ValueError("New_date column not found in the dataset")
            
        # Convert to datetime and localize to EST
        est = timezone('US/Eastern')
        df['New_date'] = pd.to_datetime(df['New_date']).dt.tz_localize(est)
        
        # Convert to UTC
        df['New_date'] = df['New_date'].dt.tz_convert('UTC')
        
        # Save transformed data
        base_path, ext = os.path.splitext('01_Data/SKMS.csv')
        output_path = f"{base_path}_transformed{ext}"
        df.to_csv(output_path, index=False)
        
        print(f"Data successfully transformed and saved to {output_path}")
        
    except FileNotFoundError:
        print("Error: The file '01_Data/SKMS.csv' was not found")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

convert_timezone()