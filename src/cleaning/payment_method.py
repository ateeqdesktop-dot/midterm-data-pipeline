import re
from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class PaymentMethodNormalizeRule(CleaningRule):
    """
    Rule 14: Normalizes and standardizes payment_method values.
    Example: 'كاش' -> 'نقد عند الاستلام', 'بطاقة' -> 'بطاقة ائتمان', 'محفظة' -> 'محفظة إلكترونية'
    """
    @property
    def rule_code(self) -> str:
        return "PAYMENT_METHOD_NORMALIZATION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        field = "payment_method"
        val = cleaned.get(field)
        
        if isinstance(val, str) and val.strip():
            original = val
            pm_str = val.strip()
            
            # Match directly or lowercase
            if pm_str in settings.PAYMENT_METHOD_SYNONYMS:
                standardized = settings.PAYMENT_METHOD_SYNONYMS[pm_str]
                if standardized != original:
                    cleaned[field] = standardized
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": standardized,
                        "rule_code": self.rule_code
                    })
            elif pm_str.lower() in settings.PAYMENT_METHOD_SYNONYMS:
                standardized = settings.PAYMENT_METHOD_SYNONYMS[pm_str.lower()]
                if standardized != original:
                    cleaned[field] = standardized
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": standardized,
                        "rule_code": self.rule_code
                    })
                    
        return cleaned, corrections
