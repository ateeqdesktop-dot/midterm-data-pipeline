import re
from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class CurrencyTextRule(CleaningRule):
    """
    Rule 2: Removes currency suffixes, prefixes, and symbols (like 'ريال', 'لاير', 'يمني', 'rial', '$', 'YER')
    from amount columns, and normalizes the 'currency' column to standard ISO codes ('YER', 'USD', 'SAR').
    """
    @property
    def rule_code(self) -> str:
        return "CURRENCY_TEXT_REMOVAL"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        # 1. Normalize currency column itself if present
        curr_val = cleaned.get("currency")
        if isinstance(curr_val, str) and curr_val.strip():
            stripped_curr = curr_val.strip()
            if stripped_curr in settings.CURRENCY_SYNONYMS:
                standard_curr = settings.CURRENCY_SYNONYMS[stripped_curr]
                if standard_curr != curr_val:
                    cleaned["currency"] = standard_curr
                    corrections.append({
                        "field": "currency",
                        "original_value": curr_val,
                        "corrected_value": standard_curr,
                        "rule_code": self.rule_code
                    })
                    
        # 2. Clean amount fields
        amount_fields = ["total_amount", "payment_amount", "delivery_cost"]
        currency_patterns = [
            r"ريال\s*يمني",
            r"ريال\s*سعودي",
            r"دولار\s*أمريكي",
            r"دولار\s*امريكي",
            r"ريال",
            r"لاير\s*يمني",
            r"لاير",
            r"يمني",
            r"سعودي",
            r"دولار",
            r"ر\.ي",
            r"ر\.س",
            r"\$",
            r"[rR]ial[s]*",
            r"[yY][eE][rR]",
            r"[uU][sS][dD]",
            r"[sS][aA][rR]"
        ]
        
        combined_pattern = re.compile("|".join(currency_patterns))
        
        inferred_currency = None
        
        for field in amount_fields:
            val = cleaned.get(field)
            if isinstance(val, str) and val.strip():
                if combined_pattern.search(val):
                    original = val
                    
                    # Detect currency hint
                    lower_val = val.lower()
                    if "$" in val or "usd" in lower_val or "دولار" in val:
                        inferred_currency = "USD"
                    elif "sar" in lower_val or "سعودي" in val or "ر.س" in val:
                        inferred_currency = "SAR"
                    elif "yer" in lower_val or "ريال" in val or "لاير" in val or "يمني" in val:
                        inferred_currency = "YER"
                        
                    cleaned_val = combined_pattern.sub("", val).strip()
                    # Clean trailing / leading spaces or slashes
                    cleaned_val = cleaned_val.strip("/- ")
                    cleaned[field] = cleaned_val
                    
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": cleaned_val,
                        "rule_code": self.rule_code
                    })
                    
        # If currency is not valid or empty, set inferred or default YER
        current_currency = cleaned.get("currency")
        if current_currency not in settings.VALID_CURRENCIES:
            target_currency = inferred_currency or settings.TARGET_CURRENCY
            cleaned["currency"] = target_currency
            corrections.append({
                "field": "currency",
                "original_value": current_currency,
                "corrected_value": target_currency,
                "rule_code": self.rule_code
            })
            
        return cleaned, corrections

