import numpy as np
from scipy.stats import shapiro

class DataValidationAgent:
    def execute(self, action, df_dict):
        """Checks skewness and normality of specified integer columns of A and B tables."""
        df_name = action.get("tables", ["EFR"])[0]
        if df_name not in df_dict:
            print(f"Table {df_name} not found.")
            return None

        df = df_dict[df_name]
        results = {}

        for col in action["columns"]:
            if col in df.columns and df[col].dtype == np.int64:
                stat, p_value = shapiro(df[col].dropna())  # Normality test
                is_normal = p_value > 0.05
                results[col] = {"p_value": p_value, "is_normal": is_normal}
                print(f"Column: {col} | P-Value: {p_value} | Normal Distribution: {is_normal}")
            else:
                print(f"Column {col} not found or not an integer column in {df_name}")
        
        return results