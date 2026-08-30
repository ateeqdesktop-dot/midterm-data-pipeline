from datetime import datetime
from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class DateNormalizeRule(CleaningRule):
    """
    Rule 7: Normalizes date formats to ISO-8601 standard string (YYYY-MM-DDTHH:MM:SS).
    If date format is invalid or cannot be parsed, it is left as-is to be handled by Quarantine.
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
            date_str = val.strip()
            
            parsed_date = None
            # Attempt to parse using supported formats
            for fmt in settings.DATE_FORMATS:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
                    
            if parsed_date:
                # Standardize to ISO format YYYY-MM-DDTHH:MM:SS
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
