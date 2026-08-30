import re
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class PhoneNormalizationRule(CleaningRule):
    """
    Rule 5: Normalizes phone numbers by removing spaces/symbols and standardizing the country code format.
    Standardized format for Yemen: +967XXXXXXXXX (where XXXXXXXXX is a 9-digit mobile number starting with 7)
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
            
            # Check for reversed group format: "4567 123 77 967+"
            # Group 1: 4 digits, Group 2: 3 digits, Group 3: 2 digits, Group 4: 3 digits followed by '+'
            reversed_match = re.match(r"^(\d{4})\s+(\d{3})\s+(\d{2})\s+(\d{3})\+$", phone_str)
            if reversed_match:
                num_clean = f"+{reversed_match.group(4)}{reversed_match.group(3)}{reversed_match.group(2)}{reversed_match.group(1)}"
            else:
                # Remove all spaces, dashes, parentheses
                # Keep digits and '+'
                num_clean = re.sub(r"[^\d+]", "", phone_str)
                
                # Handle trailing plus: "967771234567+" -> "+967771234567"
                if num_clean.endswith("+"):
                    num_clean = "+" + num_clean[:-1]
                    
                # If '+' is in the middle or end, clean it
                if "+" in num_clean[1:]:
                    num_clean = "+" + num_clean.replace("+", "")
                    
                # Normalize Yemeni country code formats:
                # 00967XXXXXXXXX -> +967XXXXXXXXX
                if num_clean.startswith("00967"):
                    num_clean = "+967" + num_clean[5:]
                # 967XXXXXXXXX (without +) -> +967XXXXXXXXX
                elif num_clean.startswith("967") and not num_clean.startswith("+"):
                    num_clean = "+967" + num_clean[3:]
                # 07XXXXXXXXX (10 digits) -> +967XXXXXXXXX
                elif num_clean.startswith("07") and len(num_clean) == 10:
                    num_clean = "+967" + num_clean[1:]
                # 7XXXXXXXX (9 digits local) -> +967XXXXXXXXX
                elif num_clean.startswith("7") and len(num_clean) == 9:
                    num_clean = "+967" + num_clean
                
            if num_clean != original:
                cleaned[field] = num_clean
                corrections.append({
                    "field": field,
                    "original_value": original,
                    "corrected_value": num_clean,
                    "rule_code": self.rule_code
                })
                
        return cleaned, corrections
