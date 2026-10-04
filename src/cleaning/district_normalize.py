import re
import difflib
from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class DistrictNormalizeRule(CleaningRule):
    """
    Rule 12: Normalizes and standardizes district/neighborhood names using dictionary mapping and fuzzy matching.
    Example: 'حده' -> 'حدة', 'القاهره' -> 'القاهرة', 'خورمكسر' -> 'خور مكسر'
    """
    @property
    def rule_code(self) -> str:
        return "DISTRICT_NORMALIZATION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        field = "district"
        val = cleaned.get(field)
        
        if isinstance(val, str) and val.strip():
            original = val
            district_str = val.strip()
            
            # 1. Exact match in VALID_DISTRICTS
            if district_str in settings.VALID_DISTRICTS:
                return cleaned, corrections
                
            # 2. Dictionary match in DISTRICT_SYNONYMS
            if district_str in settings.DISTRICT_SYNONYMS:
                standardized = settings.DISTRICT_SYNONYMS[district_str]
                cleaned[field] = standardized
                corrections.append({
                    "field": field,
                    "original_value": original,
                    "corrected_value": standardized,
                    "rule_code": self.rule_code
                })
                return cleaned, corrections
                
            # 3. Fuzzy match using difflib
            matches = difflib.get_close_matches(district_str, list(settings.VALID_DISTRICTS), n=1, cutoff=0.80)
            if matches:
                standardized = matches[0]
                cleaned[field] = standardized
                corrections.append({
                    "field": field,
                    "original_value": original,
                    "corrected_value": standardized,
                    "rule_code": self.rule_code
                })
                
        return cleaned, corrections
