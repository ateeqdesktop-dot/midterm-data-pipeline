from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class WordPriceRule(CleaningRule):
    """
    Rule 4: Converts specific known Arabic price words to their numeric strings.
    Example: "ألفان" -> "2000", "خمسة آلاف" -> "5000"
    """
    @property
    def rule_code(self) -> str:
        return "WORD_PRICE_CONVERSION"
        
    def __init__(self):
        # Dictionary of known Arabic price terms mapped to standard numeric strings
        self.words_map = {
            "ألف": "1000",
            "الف": "1000",
            "ألفين": "2000",
            "الفين": "2000",
            "ألفان": "2000",
            "الفان": "2000",
            "ثلاثة آلاف": "3000",
            "ثلاثه آلاف": "3000",
            "ثلاثة الف": "3000",
            "ثلاثه الف": "3000",
            "اربعة آلاف": "4000",
            "أربعة آلاف": "4000",
            "اربعة الف": "4000",
            "أربعة الف": "4000",
            "خمسة آلاف": "5000",
            "خمسه آلاف": "5000",
            "خمسة الف": "5000",
            "خمسه الف": "5000",
            "عشرة آلاف": "10000",
            "عشره آلاف": "10000",
            "عشرة الف": "10000",
            "عشره الف": "10000"
        }
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        numeric_fields = ["total_amount", "payment_amount", "delivery_cost"]
        
        for field in numeric_fields:
            val = cleaned.get(field)
            if isinstance(val, str) and val.strip():
                normalized_val = val.strip()
                # Remove trailing YER suffixes if any (e.g. "خمسة آلاف ريال")
                suffixes = [" ريال يمني", " ريال", " يمني", " YER", " Yer", " yer"]
                for suffix in suffixes:
                    if normalized_val.endswith(suffix):
                        normalized_val = normalized_val[:-len(suffix)].strip()
                        
                if normalized_val in self.words_map:
                    original = val
                    converted = self.words_map[normalized_val]
                    cleaned[field] = converted
                    
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": converted,
                        "rule_code": self.rule_code
                    })
                    
        return cleaned, corrections
