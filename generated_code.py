import pandas as pd
from pytz import timezone
import os

# Load dataset
try:
    df = pd.read_csv('01_Data/SKMS.csv')
except FileNotFoundError:
    raise FileNotFoundError("The file '01_Data/SKMS.csv' was not found. Please check the path.")

# Convert timezone from ET to UTC
original_tz = timezone('US/Eastern')
utc_tz = timezone('UTC')

# Assuming the timestamp column is named 'New_date'
if 'New_date' in df.columns:
    # Convert to datetime if not already
    df['New_date'] = pd.to_datetime(df['New_date'])
    
    # Localize to Eastern Time (naive -> aware)
    df['New_date'] = df['New_date'].dt.tz_localize(original_tz)
    
    # Convert to UTC
    df['New_date'] = df['New_date'].dt.tz_convert(utc_tz)
else:
    raise ValueError("Column 'New_date' not found in the dataset.")

# Save transformed data
base_path, ext = os.path.splitext('01_Data/SKMS.csv')
output_path = f"{base_path}_transformed_3434343{ext}"
df.to_csv(output_path, index=False)