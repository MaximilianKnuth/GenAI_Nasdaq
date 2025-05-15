#!/usr/bin/env python3
import os
import pandas as pd
from pathlib import Path

def load_dataframes() -> dict[str, pd.DataFrame]:
    # Use absolute path to find data files
    current_dir = os.path.dirname(os.path.abspath(__file__))
    base = Path(current_dir) / "01_Data"
    
    print(f"Loading data files from {base}")
    
    files = {
        "EFR":  base / "EFR.csv",
        "EQR":  base / "EQR.csv",
        "SKMS": base / "SKMS.csv",
    }
    
    # Check if files exist
    for name, path in files.items():
        if not path.exists():
            print(f"Data file not found: {path}")
            raise FileNotFoundError(f"Data file not found: {path}")
        else:
            print(f"Found data file: {path}")
    
    return {name: pd.read_csv(path) for name, path in files.items()}

def main():
    print("Testing data loading...")
    try:
        df_dict = load_dataframes()
        
        # Check that all dataframes were loaded
        for name, df in df_dict.items():
            print(f"\n{name} DataFrame loaded successfully with shape {df.shape}")
            print(f"Columns: {df.columns.tolist()}")
            print(f"First few rows:")
            print(df.head(3))
            
        print("\nAll data files loaded successfully!")
    except Exception as e:
        print(f"Error loading data: {e}")

if __name__ == "__main__":
    main() 