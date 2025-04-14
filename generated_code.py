import pandas as pd
import pytz
from pathlib import Path

# Define the file path
file_path = '01_Data/SKMS.csv'

# Load the dataset
try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    raise FileNotFoundError(f"The file {file_path} was not found. Please check the path.")

# Convert datetime column from UTC to EST
if 'datetime' in df.columns:
    # Convert to datetime if not already
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    # Localize as UTC (since original is UTC)
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    
    # Convert to EST
    df['datetime'] = df['datetime'].dt.tz_convert('US/Eastern')
    
    # Remove timezone info if desired (optional)
    df['datetime'] = df['datetime'].dt.tz_localize(None)
else:
    raise KeyError("The 'datetime' column was not found in the dataset.")

# Save transformed data
output_path = Path(file_path)
transformed_path = output_path.with_name(f"{output_path.stem}_transformed{output_path.suffix}")
df.to_csv(transformed_path, index=False)