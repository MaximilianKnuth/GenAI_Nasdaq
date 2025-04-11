import numpy as np
from scipy.stats import shapiro
import pandas as pd
import numpy as np
from datetime import datetime
import re
import logging

class DataValidator:
    """
    Comprehensive data validation for financial data pipelines.
    Validates data types, consistency, and quality before and during pipeline execution.
    """
    
    def __init__(self):
        # Common financial data patterns
        self.date_patterns = {
            'date': r'\d{4}-\d{2}-\d{2}',
            'datetime': r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}',
            'timestamp': r'\d{10,13}'
        }
        
        # Common financial column types
        self.financial_column_types = {
            'price': float,
            'volume': int,
            'return': float,
            'ratio': float,
            'percentage': float
        }
        
        # Validation rules for financial data
        self.validation_rules = {
            'price': lambda x: x > 0,
            'volume': lambda x: x >= 0,
            'percentage': lambda x: 0 <= x <= 100,
            'ratio': lambda x: x >= 0,
            'date': lambda x: isinstance(x, (datetime, pd.Timestamp)) or pd.to_datetime(x, errors='coerce') is not pd.NaT
        }
        
        # Expected ranges for financial metrics (for anomaly detection)
        self.expected_ranges = {
            'price': (0, 1e6),
            'volume': (0, 1e9),
            'pe_ratio': (0, 1000),
            'market_cap': (0, 3e12),
            'yield': (0, 50),
            'volatility': (0, 500)
        }
    
    def validate_dataframe(self, df, schema=None, column_types=None, rules=None):
        """
        Performs comprehensive validation on a dataframe.
        
        Args:
            df (pd.DataFrame): The dataframe to validate
            schema (dict, optional): Expected schema with column names and types
            column_types (dict, optional): Specific type requirements for columns
            rules (dict, optional): Custom validation rules
            
        Returns:
            dict: Validation results including errors, warnings, and quality metrics
        """
        if not isinstance(df, pd.DataFrame):
            return {
                'valid': False,
                'errors': ["Input is not a pandas DataFrame"],
                'warnings': [],
                'quality_metrics': {}
            }
        
        errors = []
        warnings = []
        quality_metrics = {}
        
        # Basic DataFrame checks
        quality_metrics['row_count'] = len(df)
        quality_metrics['column_count'] = len(df.columns)
        quality_metrics['memory_usage'] = df.memory_usage(deep=True).sum()
        
        # Check for empty DataFrame
        if df.empty:
            warnings.append("DataFrame is empty")
            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings,
                'quality_metrics': quality_metrics
            }
        
        # Validate against schema if provided
        if schema:
            for col, expected_type in schema.items():
                if col not in df.columns:
                    errors.append(f"Required column '{col}' is missing")
                else:
                    # Skip type checking for None values
                    non_null_values = df[col].dropna()
                    if not non_null_values.empty:
                        actual_type = type(non_null_values.iloc[0])
                        if not isinstance(non_null_values.iloc[0], expected_type):
                            errors.append(f"Column '{col}' has type {actual_type}, expected {expected_type}")
        
        # Apply specific column type validations
        column_types = column_types or {}
        for col, expected_type in column_types.items():
            if col in df.columns:
                try:
                    # Try to convert to expected type
                    pd.Series(df[col]).astype(expected_type)
                except:
                    errors.append(f"Column '{col}' cannot be converted to {expected_type}")
        
        # Data quality assessment
        for col in df.columns:
            col_quality = {}
            
            # Calculate null percentage
            null_count = df[col].isna().sum()
            null_pct = (null_count / len(df)) * 100
            col_quality['null_count'] = null_count
            col_quality['null_percentage'] = null_pct
            
            if null_pct > 20:
                warnings.append(f"Column '{col}' has {null_pct:.1f}% null values")
            
            # Calculate unique percentage
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_categorical_dtype(df[col]):
                unique_count = df[col].nunique()
                unique_pct = (unique_count / len(df)) * 100
                col_quality['unique_count'] = unique_count
                col_quality['unique_percentage'] = round(unique_pct,2)
                
                if unique_count == 1:
                    warnings.append(f"Column '{col}' has only one unique value")
                elif unique_count == len(df) and len(df) > 1:
                    warnings.append(f"Column '{col}' has all unique values, may be an ID")
            
            # Numeric column statistics
            if pd.api.types.is_numeric_dtype(df[col]):
                col_quality.update({
                    'min': round(df[col].min(),2),
                    'max': round(df[col].max(),2),
                    'mean': round(df[col].mean(),2),
                    'median': round(df[col].median(),2),
                    'std': round(df[col].std(),2)
                })
                
                # Check for suspicious numeric values
                if col_quality['min'] < 0 and 'price' in col.lower():
                    warnings.append(f"Column '{col}' has negative values but appears to be a price")
                
                # Check outliers using IQR method
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - (1.5 * iqr)
                upper_bound = q3 + (1.5 * iqr)
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
                
                if not outliers.empty:
                    col_quality['outlier_count'] = len(outliers)
                    col_quality['outlier_percentage'] = (len(outliers) / len(df)) * 100
                    
                    if col_quality['outlier_percentage'] > 5:
                        warnings.append(f"Column '{col}' has {col_quality['outlier_percentage']:.1f}% potential outliers")
            
            # Date column checks
            if pd.api.types.is_datetime64_dtype(df[col]) or any(pattern in col.lower() for pattern in ['date', 'time', 'timestamp']):
                try:
                    # Try to convert to datetime if not already
                    if not pd.api.types.is_datetime64_dtype(df[col]):
                        date_series = pd.to_datetime(df[col], errors='coerce')
                        invalid_dates = date_series.isna().sum()
                        if invalid_dates > 0:
                            warnings.append(f"Column '{col}' has {invalid_dates} values that can't be parsed as dates")
                        
                        # Check for future dates
                        future_dates = date_series[date_series > pd.Timestamp.now()]
                        if not future_dates.empty:
                            warnings.append(f"Column '{col}' has {len(future_dates)} future dates")
                except:
                    warnings.append(f"Column '{col}' has name suggesting date but can't be converted to datetime")
            
            quality_metrics[col] = col_quality
        
        # Apply custom validation rules
        rules = rules or {}
        for col, rule_func in rules.items():
            if col in df.columns:
                try:
                    valid_mask = df[col].apply(rule_func)
                    invalid_count = (~valid_mask).sum()
                    if invalid_count > 0:
                        errors.append(f"Column '{col}' has {invalid_count} values that fail validation rules")
                except Exception as e:
                    errors.append(f"Error applying validation rule to column '{col}': {str(e)}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'quality_metrics': quality_metrics
        }
    
    def validate_code_safety(self, code):
        """
        Validates code for potential security issues.
        
        Args:
            code (str): The code to validate
            
        Returns:
            dict: Validation results including any security warnings
        """
        security_issues = []
        
        # Check for potentially dangerous imports
        dangerous_imports = ['os.system', 'subprocess', 'eval', 'exec', '__import__']
        for imp in dangerous_imports:
            if imp in code:
                security_issues.append(f"Code contains potentially dangerous operation: {imp}")
        
        # Check for file operations
        file_operations = ['open(', 'write(', 'read(', 'os.remove', 'os.unlink']
        for op in file_operations:
            if op in code:
                security_issues.append(f"Code contains file operation: {op}")
        
        # Check for web/network access
        network_operations = ['requests.', 'urllib', 'socket.', 'http.']
        for op in network_operations:
            if op in code:
                security_issues.append(f"Code contains network operation: {op}")
        
        return {
            'safe': len(security_issues) == 0,
            'issues': security_issues
        }
    
    def infer_column_types(self, df):
        """
        Intelligently infers column types from a dataframe with financial context awareness.
        
        Args:
            df (pd.DataFrame): The dataframe to analyze
            
        Returns:
            dict: Inferred column types and categories
        """
        column_types = {}
        financial_columns = {
            'date': [],
            'price': [],
            'volume': [],
            'identifier': [],
            'categorical': [],
            'numeric': [],
            'text': []
        }
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Check for date columns
            if pd.api.types.is_datetime64_dtype(df[col]) or any(term in col_lower for term in ['date', 'time', 'day', 'month', 'year', 'timestamp']):
                try:
                    pd.to_datetime(df[col], errors='raise')
                    column_types[col] = 'datetime'
                    financial_columns['date'].append(col)
                    continue
                except:
                    pass
            
            # Check for price columns
            if any(term in col_lower for term in ['price', 'cost', 'value', 'amount', 'close', 'open', 'high', 'low']):
                if pd.api.types.is_numeric_dtype(df[col]):
                    column_types[col] = 'price'
                    financial_columns['price'].append(col)
                    continue
            
            # Check for volume columns
            if any(term in col_lower for term in ['volume', 'quantity', 'size', 'count']):
                if pd.api.types.is_numeric_dtype(df[col]):
                    column_types[col] = 'volume'
                    financial_columns['volume'].append(col)
                    continue
            
            # Check for identifier columns
            if any(term in col_lower for term in ['id', 'code', 'symbol', 'ticker', 'cusip', 'isin', 'sedol']):
                column_types[col] = 'identifier'
                financial_columns['identifier'].append(col)
                continue
            
            # General type inference
            if pd.api.types.is_numeric_dtype(df[col]):
                column_types[col] = 'numeric'
                financial_columns['numeric'].append(col)
            elif df[col].nunique() < 20 and len(df) > 20:
                column_types[col] = 'categorical'
                financial_columns['categorical'].append(col)
            else:
                column_types[col] = 'text'
                financial_columns['text'].append(col)
        
        return {
            'column_types': column_types,
            'financial_columns': financial_columns
        }
    
    def validate_financial_data_integrity(self, df, dataset_type=None):
        """
        Validates financial data integrity with domain-specific checks.
        
        Args:
            df (pd.DataFrame): The dataframe to validate
            dataset_type (str, optional): Type of financial dataset for specialized checks
            
        Returns:
            dict: Financial validation results
        """
        errors = []
        warnings = []
        integrity_metrics = {}
        
        # Infer column types
        column_info = self.infer_column_types(df)
        financial_columns = column_info['financial_columns']
        
        # Perform dataset-specific validations
        if dataset_type == 'market_data':
            # Validate price data
            price_cols = financial_columns['price']
            if price_cols:
                for col in price_cols:
                    # Check for negative prices
                    neg_prices = (df[col] < 0).sum()
                    if neg_prices > 0:
                        errors.append(f"Column '{col}' has {neg_prices} negative prices")
                    
                    # Check for extreme price changes (if date column exists)
                    if financial_columns['date']:
                        date_col = financial_columns['date'][0]
                        try:
                            sorted_df = df.sort_values(date_col)
                            price_changes = sorted_df[col].pct_change().abs()
                            extreme_changes = price_changes[price_changes > 0.5]  # 50% change
                            
                            if not extreme_changes.empty:
                                warnings.append(f"Column '{col}' has {len(extreme_changes)} instances of price changes >50%")
                                integrity_metrics[f'{col}_extreme_changes'] = len(extreme_changes)
                        except:
                            pass
            
            # Validate volume data
            volume_cols = financial_columns['volume']
            if volume_cols:
                for col in volume_cols:
                    # Check for negative volumes
                    neg_volumes = (df[col] < 0).sum()
                    if neg_volumes > 0:
                        errors.append(f"Column '{col}' has {neg_volumes} negative volumes")
            
            # Check for price/volume consistency
            if price_cols and volume_cols:
                # Simple correlation check
                try:
                    price_col = price_cols[0]
                    volume_col = volume_cols[0]
                    correlation = df[price_col].corr(df[volume_col])
                    integrity_metrics['price_volume_correlation'] = round(correlation,2)
                except:
                    pass
        
        elif dataset_type == 'financial_statement':
            # Balance sheet validations
            balance_sheet_items = {
                'assets': ['asset', 'cash', 'receivable', 'inventory', 'property', 'equipment'],
                'liabilities': ['liabilit', 'payable', 'debt', 'loan'],
                'equity': ['equity', 'capital', 'stock', 'retained']
            }
            
            # Check for basic accounting equation: Assets = Liabilities + Equity
            assets = [col for col in df.columns if any(item in col.lower() for item in balance_sheet_items['assets'])]
            liabilities = [col for col in df.columns if any(item in col.lower() for item in balance_sheet_items['liabilities'])]
            equity = [col for col in df.columns if any(item in col.lower() for item in balance_sheet_items['equity'])]
            
            if assets and liabilities and equity:
                try:
                    # Simple check for accounting equation
                    assets_sum = df[assets].sum(axis=1)
                    liab_equity_sum = df[liabilities + equity].sum(axis=1)
                    
                    # Allow small differences due to rounding
                    diff_pct = ((assets_sum - liab_equity_sum).abs() / assets_sum) * 100
                    large_diffs = diff_pct[diff_pct > 1]  # >1% difference
                    
                    if not large_diffs.empty:
                        warnings.append(f"Found {len(large_diffs)} rows with accounting equation discrepancies >1%")
                        integrity_metrics['accounting_equation_violations'] = len(large_diffs)
                except:
                    pass
        
        # General financial data checks
        
        # Check for date continuity if time series
        if financial_columns['date']:
            date_col = financial_columns['date'][0]
            try:
                df_copy = df.copy()
                df_copy['date_temp'] = pd.to_datetime(df[date_col])
                
                # Check if identifier columns exist
                id_cols = financial_columns['identifier']
                
                if id_cols:
                    # Check continuity per identifier
                    id_col = id_cols[0]
                    
                    # Group by identifier and check date gaps
                    for identifier, group in df_copy.groupby(id_col):
                        sorted_group = group.sort_values('date_temp')
                        
                        if len(sorted_group) <= 1:
                            continue
                        
                        # Check for date gaps
                        date_diff = sorted_group['date_temp'].diff().dropna()
                        
                        # Get the most common diff as the expected frequency
                        most_common_diff = date_diff.value_counts().idxmax()
                        
                        # Find gaps (diffs significantly larger than the most common)
                        gaps = date_diff[date_diff > most_common_diff * 2]
                        
                        if not gaps.empty:
                            warnings.append(f"Identifier '{identifier}' has {len(gaps)} potential date gaps")
                else:
                    # Check overall continuity
                    sorted_df = df_copy.sort_values('date_temp')
                    date_diff = sorted_df['date_temp'].diff().dropna()
                    
                    # Get the most common diff as the expected frequency
                    most_common_diff = date_diff.value_counts().idxmax()
                    
                    # Find gaps (diffs significantly larger than the most common)
                    gaps = date_diff[date_diff > most_common_diff * 2]
                    
                    if not gaps.empty:
                        warnings.append(f"Dataset has {len(gaps)} potential date gaps")
            except Exception as e:
                warnings.append(f"Could not validate date continuity: {str(e)}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'integrity_metrics': integrity_metrics,
            'column_categories': financial_columns
        }
