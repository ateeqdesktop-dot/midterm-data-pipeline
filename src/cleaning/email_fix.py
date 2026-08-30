import re
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class EmailFixRule(CleaningRule):
    """
    Rule 6: Fixes obvious typographical repeating symbols in emails.
    Example: "user@@mail..com" -> "user@mail.com"
    """
    @property
    def rule_code(self) -> str:
        return "EMAIL_REPEATED_SYMBOLS"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        field = "customer_email"
        val = cleaned.get(field)
        
        if isinstance(val, str) and val.strip():
            original = val
            email = val.strip()
            
            # Replace multiple @ signs (e.g. @@ or @@@) with a single @
            email_fixed_at = re.sub(r"@+", "@", email)
            
            # Replace multiple dots (e.g. .. or ...) with a single dot
            email_fixed_dots = re.sub(r"\.+", ".", email_fixed_at)
            
            # Clean up leading/trailing dots or spaces
            email_cleaned = email_fixed_dots.strip(".")
            
            if email_cleaned != original:
                cleaned[field] = email_cleaned
                corrections.append({
                    "field": field,
                    "original_value": original,
                    "corrected_value": email_cleaned,
                    "rule_code": self.rule_code
                })
                
        return cleaned, corrections
