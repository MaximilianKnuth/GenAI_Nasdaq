import pandas as pd
from pytz import timezone

def convert_datetime():
    try:
        # Read the SKMS table
        skms = pd.read_csv('01_Data/SKMS.csv')
        
        # Convert New_date from EST to UTC
        est = timezone('US/Eastern')
        utc = timezone('UTC')
        
        skms['New_date'] = pd.to_datetime(skms['New_date'])
        skms['New_date'] = skms['New_date'].dt.tz_localize(est).dt.tz_convert(utc)
        
        # Save the converted dataframe
        skms.to_csv('SKMS_converted.csv', index=False)
        return skms
    
    except Exception as e:
        print(f"Error in datetime conversion: {str(e)}")
        return None

def join_tables(skms_converted):
    try:
        if skms_converted is None:
            raise ValueError("No converted SKMS data available")
            
        # Read EFR table
        efr = pd.read_csv('01_Data/EFR.csv')
        
        # Join tables on ticker column
        merged = pd.merge(skms_converted, efr, on='ticker', how='inner')
        
        # Save the merged dataframe
        merged.to_csv('SKMS_EFR_merged.csv', index=False)
        return merged
    
    except Exception as e:
        print(f"Error in joining tables: {str(e)}")
        return None

# Execute the steps sequentially
if __name__ == "__main__":
    converted_skms = convert_datetime()
    if converted_skms is not None:
        join_tables(converted_skms)