class TableJoinAgent:
    def execute(self, action, df_dict):
        """Joins two tables based on the ticker column."""
        table_a, table_b = action["tables"]
        join_key = action["join_on"]

        if table_a not in df_dict or table_b not in df_dict:
            print(f"One or both tables ({table_a}, {table_b}) not found.")
            return None

        df_a = df_dict[table_a]
        df_b = df_dict[table_b]

        if join_key in df_a.columns and join_key in df_b.columns:
            merged_df = df_a.merge(df_b, on=join_key, how="inner")
            print(f"Joined {table_a} and {table_b} on {join_key}")
            return merged_df
        else:
            print(f"Join key {join_key} not found in one or both tables.")
            return None