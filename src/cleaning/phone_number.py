import re
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class PhoneNormalizationRule(CleaningRule):
    """
    Rule 5: Normalizes phone numbers by removing spaces/symbols and standardizing to international format.
    Standardized format for Yemen mobile numbers: +967XXXXXXXXX (9 digits, starts with 7)
    Standardized format for Yemen landline numbers: +967XXXXXXXX (starts with 1, 2, 3, 4...)
    """
    @property
    def rule_code(self) -> str:
        return "PHONE_NORMALIZATION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        field = "customer_phone"
        val = cleaned.get(field)
        
        if isinstance(val, str) and val.strip():
            original = val
            phone_str = val.strip()
            
            # 1. Check for reversed group formats (common RTL rendering artifacts in CSV):
            # Example: "4567 123 77 967+" -> "+967771234567"
            reversed_match_4 = re.match(r"^(\d{4})\s+(\d{3})\s+(\d{2})\s+(\d{3})\+$", phone_str)
            reversed_match_3 = re.match(r"^(\d{3,4})\s+(\d{3})\s+(\d{2,3})\s+(\d{3})\+$", phone_str)
            
            if reversed_match_4:
                num_clean = f"+{reversed_match_4.group(4)}{reversed_match_4.group(3)}{reversed_match_4.group(2)}{reversed_match_4.group(1)}"
            elif reversed_match_3:
                num_clean = f"+{reversed_match_3.group(4)}{reversed_match_3.group(3)}{reversed_match_3.group(2)}{reversed_match_3.group(1)}"
            else:
                # Remove all non-digits except '+'
                num_clean = re.sub(r"[^\d+]", "", phone_str)
                
                # Handle trailing plus: "967771234567+" -> "+967771234567"
                if num_clean.endswith("+"):
                    num_clean = "+" + num_clean[:-1]
                    
                # If '+' is elsewhere, normalize to start
                if "+" in num_clean:
                    num_clean = "+" + num_clean.replace("+", "")
                    
                # Normalize country code variations:
                # 00967XXXXXXXXX -> +967XXXXXXXXX
                if num_clean.startswith("+00967"):
                    num_clean = "+967" + num_clean[6:]
                elif num_clean.startswith("00967"):
                    num_clean = "+967" + num_clean[5:]
                elif num_clean.startswith("967") and not num_clean.startswith("+"):
                    num_clean = "+967" + num_clean[3:]
                elif num_clean.startswith("+967"):
                    pass # already has +967
                # 07XXXXXXXX (10 digits starting with 07) -> +9677XXXXXXXX
                elif num_clean.startswith("07") and len(num_clean) == 10:
                    num_clean = "+967" + num_clean[1:]
                # 7XXXXXXXX (9 digits starting with 7: 70, 71, 73, 77, 78...) -> +967XXXXXXXXX
                elif num_clean.startswith("7") and len(num_clean) == 9:
                    num_clean = "+967" + num_clean
                # 01XXXXXXX (landline Sanaa) / 02XXXXXXX (Aden) -> +9671XXXXXXX
                elif num_clean.startswith("0") and len(num_clean) in (8, 9):
                    num_clean = "+967" + num_clean[1:]
                
            if num_clean != original and num_clean.startswith("+967"):
                cleaned[field] = num_clean
                corrections.append({
                    "field": field,
                    "original_value": original,
                    "corrected_value": num_clean,
                    "rule_code": self.rule_code
                })
                
        return cleaned, corrections

