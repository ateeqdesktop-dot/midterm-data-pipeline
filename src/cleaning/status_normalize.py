import re
from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class StatusNormalizeRule(CleaningRule):
    """
    Rule 8: Normalizes order status, payment status, payment method, and trims whitespaces / invisible chars in textual fields.
    Maps synonyms to standard statuses (e.g. "تم التسليم" -> "توصيل", "قيد الشحن" -> "شحن", "بانتظار" -> "قيد الانتظار").
    """
    @property
    def rule_code(self) -> str:
        return "STATUS_WHITESPACE_SYNONYM"
        
    def _clean_text(self, text: str) -> str:
        # Remove zero-width spaces, byte-order-marks, tatweel (kashida), and trim
        cleaned = re.sub(r"[\u200B-\u200D\uFEFF\u0640]", "", text)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        # 1. Clean and trim whitespace for all string columns
        for key, val in list(cleaned.items()):
            if isinstance(val, str):
                cleaned_str = self._clean_text(val)
                if cleaned_str != val:
                    cleaned[key] = cleaned_str
                    corrections.append({
                        "field": key,
                        "original_value": val,
                        "corrected_value": cleaned_str,
                        "rule_code": "TRIM_WHITESPACE"
                    })
                    
        # 2. Normalize status and payment_status values
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

