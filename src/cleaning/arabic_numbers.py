from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class ArabicNumbersRule(CleaningRule):
    """
    Rule 1: Converts Eastern Arabic numerals (٠-٩) and Arabic decimal comma (٫)
    to Western/Latin digits (0-9) and standard decimal point (.) in numeric columns
    and phone numbers.
    """
    @property
    def rule_code(self) -> str:
        return "ARABIC_NUMERALS"
        
    def _convert(self, val_str: str) -> str:
        arabic_digits = "٠١٢٣٤٥٦٧٨٩٫"
        latin_digits = "0123456789."
        trans_table = str.maketrans(arabic_digits, latin_digits)
        return val_str.translate(trans_table)
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        # Fields where we expect numeric content or digits
        numeric_fields = ["total_amount", "payment_amount", "delivery_cost", "customer_phone"]
        
        for field in numeric_fields:
            val = cleaned.get(field)
            if isinstance(val, str) and any(char in val for char in "٠١٢٣٤٥٦٧٨٩٫"):
                original = val
                converted = self._convert(val)
                cleaned[field] = converted
                
                corrections.append({
                    "field": field,
                    "original_value": original,
                    "corrected_value": converted,
                    "rule_code": self.rule_code
                })
                
        return cleaned, corrections
