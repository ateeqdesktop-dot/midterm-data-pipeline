from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class ArabicNumbersRule(CleaningRule):
    """
    Rule 1: Converts Eastern Arabic numerals (٠-٩), Persian digits (۰-۹), and Arabic decimal comma (٫)
    to Western/Latin digits (0-9) and standard decimal point (.) across all record fields
    (including numeric columns, phone numbers, order dates, IDs, and items JSON).
    """
    @property
    def rule_code(self) -> str:
        return "ARABIC_NUMERALS"
        
    def _convert(self, val_str: str) -> str:
        arabic_digits = "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫"
        latin_digits = "01234567890123456789."
        trans_table = str.maketrans(arabic_digits, latin_digits)
        return val_str.translate(trans_table)
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        target_chars = set("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫")
        
        for field, val in list(cleaned.items()):
            if isinstance(val, str) and any(char in target_chars for char in val):
                original = val
                converted = self._convert(val)
                if converted != original:
                    cleaned[field] = converted
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": converted,
                        "rule_code": self.rule_code
                    })
                
        return cleaned, corrections

