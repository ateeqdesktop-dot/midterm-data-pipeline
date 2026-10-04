import re
from datetime import datetime
from typing import Dict, Any, List, Tuple
try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None

from config import settings
from src.cleaning.base_rule import CleaningRule

class DateNormalizeRule(CleaningRule):
    """
    Rule 7: Normalizes various date string formats to ISO-8601 standard string (YYYY-MM-DDTHH:MM:SS).
    Handles dash/slash/dot delimiters, 12-hour AM/PM and Arabic ص/م markers, and utilizes dateutil.
    If the date is impossible or cannot be parsed, it is preserved for quarantine classification.
    """
    @property
    def rule_code(self) -> str:
        return "DATE_FORMAT_NORMALIZATION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        field = "order_date"
        val = cleaned.get(field)
        
        if isinstance(val, str) and val.strip():
            original = val
            date_str = val.strip().strip("'\"`")
            
            # Normalize internal whitespaces and replace Arabic AM/PM markers
            date_str = re.sub(r"\s+", " ", date_str)
            date_str = re.sub(r"\bصباح[اًا]*|\bص\b", "AM", date_str)
            date_str = re.sub(r"\bمساء[اًا]*|\bم\b", "PM", date_str)
            
            parsed_date = None
            
            # 1. Fast check if already in standard ISO format YYYY-MM-DDTHH:MM:SS
            if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", date_str):
                try:
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    parsed_date = None
                    
            # 2. Try configured date formats
            if not parsed_date:
                for fmt in settings.DATE_FORMATS:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue
                        
            # 3. Try dateutil parser if available (with dayfirst=True heuristics for non-ISO)
            if not parsed_date and dateutil_parser is not None:
                # Do a quick sanity check to avoid parsing crazy strings like "2025-19-45 99:70:00"
                # which dateutil might reject or raise error
                try:
                    parsed_date = dateutil_parser.parse(date_str, dayfirst=True)
                except Exception:
                    parsed_date = None
                    
            # 4. If parsed a valid datetime
            if parsed_date:
                # Plausibility check on year (avoid unreasonable values like 0001 or 9999)
                if 1990 <= parsed_date.year <= 2050:
                    standardized = parsed_date.strftime("%Y-%m-%dT%H:%M:%S")
                    if standardized != original:
                        cleaned[field] = standardized
                        corrections.append({
                            "field": field,
                            "original_value": original,
                            "corrected_value": standardized,
                            "rule_code": self.rule_code
                        })
                    
        return cleaned, corrections


