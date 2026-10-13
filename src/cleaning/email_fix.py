import re
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class EmailFixRule(CleaningRule):
    """
    Rule 6: Fixes typographical errors, whitespace, and repeating symbols in emails.
    Example: "user@@mail..com" -> "user@mail.com", "<user @ example,com>" -> "user@example.com"
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
            email = val.strip().strip("<>\"'`;")
            
            # Remove mailto: prefix if present
            if email.lower().startswith("mailto:"):
                email = email[7:].strip()
                
            # Remove all internal spaces (e.g. user @ example . com)
            email = re.sub(r"\s+", "", email)
            
            # Replace multiple @ signs (e.g. @@ or @@@) with a single @
            email = re.sub(r"@+", "@", email)
            
            # Replace comma typo in domain part (e.g. @example,com -> @example.com)
            if "@" in email:
                user_part, domain_part = email.split("@", 1)
                domain_part = domain_part.replace(",", ".")
                email = f"{user_part}@{domain_part}"
            else:
                email = email.replace(",", ".")
                
            # Replace multiple dots (e.g. .. or ...) with a single dot
            email = re.sub(r"\.+", ".", email)
            
            # Clean up leading/trailing dots or hyphens
            email = email.strip(".-_")
            
            if email != original and email:
                cleaned[field] = email
                corrections.append({
                    "field": field,
                    "original_value": original,
                    "corrected_value": email,
                    "rule_code": self.rule_code
                })
                
        return cleaned, corrections

