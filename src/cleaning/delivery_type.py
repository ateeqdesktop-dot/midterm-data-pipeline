import re
from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class DeliveryTypeNormalizeRule(CleaningRule):
    """
    Rule 13: Normalizes and standardizes delivery_type ('سريع' / 'عادي').
    Example: 'توصيل سريع' -> 'سريع', 'express' -> 'سريع', 'standard' -> 'عادي'
    """
    @property
    def rule_code(self) -> str:
        return "DELIVERY_TYPE_NORMALIZATION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        field = "delivery_type"
        val = cleaned.get(field)
        
        if isinstance(val, str) and val.strip():
            original = val
            d_type = val.strip().lower()
            
            if d_type in settings.DELIVERY_TYPE_SYNONYMS:
                standardized = settings.DELIVERY_TYPE_SYNONYMS[d_type]
                if standardized != original:
                    cleaned[field] = standardized
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": standardized,
                        "rule_code": self.rule_code
                    })
                    
        return cleaned, corrections
