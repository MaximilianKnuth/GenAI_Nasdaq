import pandas as pd
from pytz import timezone
import os

def convert_datetime():
    try:
        # Load SKMS table
        skms = pd.read_csv('01_Data/SKMS.csv')
        
        # Convert New_date from EST to UTC
        est = timezone('US/Eastern')
        utc = timezone('UTC')
        
        # Convert to datetime and localize to EST
        skms['New_date'] = pd.to_datetime(skms['New_date'])
        skms['New_date'] = skms['New_date'].dt.tz_localize(est)
        
        # Convert to UTC
        skms['New_date'] = skms['New_date'].dt.tz_convert(utc)
        
        # Remove timezone info for cleaner output
        skms['New_date'] = skms['New_date'].dt.tz_localize(None)
        
        # Save intermediate result
        skms.to_csv('temp_skms_utc.csv', index=False)
        return skms
        
    except Exception as e:
        print(f"Error in datetime conversion: {str(e)}")
        return None

def join_tables(skms_data):
    try:
        # Load EFR table
        efr = pd.read_csv('01_Data/EFR.csv')
        
        # Perform inner join on ticker column
        merged = pd.merge(skms_data, efr, on='ticker', how='inner')
        
        # Save final output
        merged.to_csv('final_output.csv', index=False)
        return merged
        
    except Exception as e:
        print(f"Error in joining tables: {str(e)}")
        return None

def main():
    # Step 1: Convert datetime
    skms_converted = convert_datetime()
    if skms_converted is None:
        print("Datetime conversion failed. Exiting.")
        return
    
    # Step 2: Join tables
    final_result = join_tables(skms_converted)
    if final_result is None:
        print("Table joining failed. Exiting.")
        return
    
    print("Operation completed successfully. Output saved to final_output.csv")

if __name__ == "__main__":
    main()