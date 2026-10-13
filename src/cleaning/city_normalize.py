import re
import difflib
from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class CityNormalizeRule(CleaningRule):
    """
    Rule 10: Normalizes and standardizes Yemeni city and governorate names
    using dictionary synonyms and fuzzy string matching.
    Example: 'صنعا' -> 'صنعاء', 'الحديده' -> 'الحديدة', 'مارب' -> 'مأرب'
    """
    @property
    def rule_code(self) -> str:
        return "CITY_NORMALIZATION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        field = "city"
        val = cleaned.get(field)
        
        if isinstance(val, str) and val.strip():
            original = val
            city_str = val.strip()
            
            # 1. Exact match
            if city_str in settings.VALID_CITIES:
                return cleaned, corrections
                
            # 2. Dictionary match
            if city_str in settings.CITY_SYNONYMS:
                standardized = settings.CITY_SYNONYMS[city_str]
                if standardized != original:
                    cleaned[field] = standardized
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": standardized,
                        "rule_code": self.rule_code
                    })
                return cleaned, corrections
                
            # 3. Fuzzy match
            matches = difflib.get_close_matches(city_str, list(settings.VALID_CITIES), n=1, cutoff=0.80)
            if matches:
                standardized = matches[0]
                if standardized != original:
                    cleaned[field] = standardized
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": standardized,
                        "rule_code": self.rule_code
                    })
                    
        return cleaned, corrections

