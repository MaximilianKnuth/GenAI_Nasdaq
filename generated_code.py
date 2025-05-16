import pandas as pd
import pytz
from datetime import datetime
import sys

def convert_datetime():
    try:
        # Load SKMS data
        skms = pd.read_csv("01_Data/SKMS.csv")
        
        # Convert New_date from EST to UTC
        est = pytz.timezone('US/Eastern')
        utc = pytz.utc
        
        skms['New_date'] = pd.to_datetime(skms['New_date'])
        skms['New_date'] = skms['New_date'].dt.tz_localize(est).dt.tz_convert(utc)
        
        # Save intermediate result
        skms.to_csv("skms_converted.csv", index=False)
        return skms
        
    except Exception as e:
        print(f"Error in convert_datetime: {str(e)}")
        sys.exit(1)

def join_tables(skms_converted):
    try:
        # Load EFR data
        efr = pd.read_csv("01_Data/EFR.csv")
        
        # Join tables on ticker column
        merged = pd.merge(skms_converted, efr, on='ticker', how='inner')
        
        # Save final output
        merged.to_csv("output.csv", index=False)
        return merged
        
    except Exception as e:
        print(f"Error in join_tables: {str(e)}")
        sys.exit(1)

# Execute steps sequentially
converted_skms = convert_datetime()
join_tables(converted_skms)