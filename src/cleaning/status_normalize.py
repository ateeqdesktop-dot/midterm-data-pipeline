from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class StatusNormalizeRule(CleaningRule):
    """
    Rule 8: Normalizes order status, payment status, and trims whitespaces in textual fields.
    Maps synonyms to standard statuses (e.g. "بانتظار" -> "قيد الانتظار").
    """
    @property
    def rule_code(self) -> str:
        return "STATUS_WHITESPACE_SYNONYM"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        # 1. Trim whitespace for all string columns
        for key, val in cleaned.items():
            if isinstance(val, str):
                trimmed = val.strip()
                if trimmed != val:
                    cleaned[key] = trimmed
                    corrections.append({
                        "field": key,
                        "original_value": val,
                        "corrected_value": trimmed,
                        "rule_code": "TRIM_WHITESPACE"
                    })
                    
        # 2. Normalize status values
        status_fields = ["status", "payment_status"]
        for field in status_fields:
            val = cleaned.get(field)
            if isinstance(val, str) and val in settings.VALID_STATUSES:
                standardized = settings.VALID_STATUSES[val]
                if standardized != val:
                    cleaned[field] = standardized
                    corrections.append({
                        "field": field,
                        "original_value": val,
                        "corrected_value": standardized,
                        "rule_code": self.rule_code
                    })
                    
        return cleaned, corrections
