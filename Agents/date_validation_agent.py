import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import re

class DateValidationAgent:
    def __init__(self,
                 max_missing_threshold: float = 0.05,
                 strict: bool = True,
                 repair_enabled: bool = True,
                 logger: Optional[logging.Logger] = None):
        """
        Enhanced DateValidationAgent with auto-repair, reasoning logs, and smart validation.

        Args:
            max_missing_threshold: Allowed proportion of missing values.
            strict: If True, columns must have 0 invalids and < threshold missing to pass.
            repair_enabled: If True, attempts to clean/repair faulty columns.
            logger: Optional logging object.
        """
        self.name = "SmartDateValidationAgent"
        self.max_missing_threshold = max_missing_threshold
        self.strict = strict
        self.repair_enabled = repair_enabled
        self.history = []
        self.logger = logger or logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)

    def describe(self) -> str:
        return (
            f"{self.name} – Agent to validate and repair datetime columns.\n"
            f"- Max missing allowed: {self.max_missing_threshold * 100:.1f}%\n"
            f"- Strict mode: {'Yes' if self.strict else 'No'}\n"
            f"- Repair enabled: {'Yes' if self.repair_enabled else 'No'}"
        )

    def _is_natural_language_date(self, val: str) -> bool:
        val = str(val).lower()
        return any(kw in val for kw in ['yesterday', 'today', 'now', 'next', 'last', 'tomorrow'])

    def _reason_about_column(self, series: pd.Series) -> str:
        """
        Provides reasoning based on the column content.
        """
        if series.empty:
            return "Empty column — cannot assess datetime quality."

        samples = series.dropna().astype(str).sample(min(3, len(series))).tolist()
        return f"Sample values: {samples}. May indicate format inconsistency or NL strings."

    def _attempt_repair(self, series: pd.Series) -> pd.Series:
        """
        Attempts to auto-clean ambiguous or partially invalid datetime columns.
        """
        if not self.repair_enabled:
            return series

        # Strip text, normalize date-like patterns
        cleaned = series.astype(str).str.extract(r'(\d{4}[-/]\d{1,2}[-/]\d{1,2}.*?)')[0]
        fallback = pd.to_datetime(cleaned, errors='coerce')
        if fallback.notna().sum() > 0.5 * len(series):
            return fallback
        return pd.to_datetime(series, errors='coerce')  # best-effort fallback

    def validate_date_column(self, df: pd.DataFrame, col: str) -> Dict[str, Any]:
        """
        Validates and (optionally) repairs a datetime column.
        """
        result = {
            "column": col,
            "valid": False,
            "repaired": False,
            "missing_ratio": None,
            "invalid_count": None,
            "natural_language_detected": False,
            "message": "",
            "recommendation": "",
        }

        try:
            series = df[col]

            # Detect NL expressions
            if series.dropna().astype(str).apply(self._is_natural_language_date).any():
                result["natural_language_detected"] = True

            # Try conversion
            dt = pd.to_datetime(series, errors='coerce')
            missing = dt.isna().sum()
            result["missing_ratio"] = missing / len(series)
            result["invalid_count"] = (dt.isna() & series.notna()).sum()

            # Decision logic
            if self.strict:
                result["valid"] = result["invalid_count"] == 0 and result["missing_ratio"] <= self.max_missing_threshold
            else:
                result["valid"] = result["missing_ratio"] <= 0.5

            # Reason and optionally repair
            if not result["valid"] and self.repair_enabled:
                repaired = self._attempt_repair(series)
                repaired_missing = repaired.isna().sum() / len(series)
                if repaired_missing < result["missing_ratio"]:
                    result["repaired"] = True
                    df[col] = repaired
                    result["message"] = "Auto-repaired column with reduced missing rate."
                    result["valid"] = repaired_missing <= self.max_missing_threshold
                else:
                    result["message"] = "Repair attempted but insufficient improvement."
            elif result["valid"]:
                result["message"] = "Column is valid datetime."
            else:
                result["message"] = "Column invalid and repair disabled or ineffective."

            if not result["valid"]:
                result["recommendation"] = self._reason_about_column(series)

        except Exception as e:
            result["message"] = f"Exception during validation: {e}"

        self.logger.info(f"[{col}] Validation Result: {result}")
        self.history.append(result)
        return result

    def validate_columns(self, df: pd.DataFrame, columns: List[str]) -> List[Dict[str, Any]]:
        return [self.validate_date_column(df, col) for col in columns]

    def get_history(self) -> List[Dict[str, Any]]:
        return self.history

    def summary(self) -> pd.DataFrame:
        """
        Returns a dataframe summary of all validations.
        """
        return pd.DataFrame(self.history)
