import re
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class CurrencyTextRule(CleaningRule):
    """
    Rule 2: Removes currency suffixes or text (like 'ريال', 'لاير', 'يمني', 'rial')
    from amount columns and ensures the 'currency' column is set to 'YER' when applicable.
    """
    @property
    def rule_code(self) -> str:
        return "CURRENCY_TEXT_REMOVAL"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        amount_fields = ["total_amount", "payment_amount", "delivery_cost"]
        currency_patterns = [
            r"\s*ريال\s*يمني",
            r"\s*ريال",
            r"\s*لاير\s*يمني",
            r"\s*لاير",
            r"\s*يمني",
            r"\s*[rR]ial[s]*",
            r"\s*[yY][eE][rR]"
        ]
        
        # Combine patterns into regex
        combined_pattern = re.compile("|".join(currency_patterns))
        
        for field in amount_fields:
            val = cleaned.get(field)
            if isinstance(val, str) and val.strip():
                # Check if it matches any currency text
                if combined_pattern.search(val):
                    original = val
                    # Remove the currency suffix/prefix
                    cleaned_val = combined_pattern.sub("", val).strip()
                    cleaned[field] = cleaned_val
                    
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": cleaned_val,
                        "rule_code": self.rule_code
                    })
                    
                    # If this is total_amount or payment_amount, we can normalize currency to YER if it's not set
                    if cleaned.get("currency") not in ["YER", "USD", "SAR"]:
                        old_currency = cleaned.get("currency")
                        cleaned["currency"] = "YER"
                        corrections.append({
                            "field": "currency",
                            "original_value": old_currency,
                            "corrected_value": "YER",
                            "rule_code": self.rule_code
                        })
                        
        return cleaned, corrections
