import re
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class ThousandsSeparatorRule(CleaningRule):
    """
    Rule 3: Removes thousands separator commas from numeric fields.
    Example: "125,000.00" -> "125000.00"
    """
    @property
    def rule_code(self) -> str:
        return "THOUSANDS_SEPARATOR"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        numeric_fields = ["total_amount", "payment_amount", "delivery_cost"]
        
        # Match a comma followed by 3 digits (standard thousands separator pattern)
        # We can also just check if comma is inside the string and replace it, but we should be careful.
        # Let's replace any comma that is surrounded by digits.
        comma_pattern = re.compile(r"(\d),(\d{3})")
        
        for field in numeric_fields:
            val = cleaned.get(field)
            if isinstance(val, str) and "," in val:
                original = val
                # Keep replacing until no more matches (handles multiple thousands separators like 1,234,567.89)
                current = val
                while True:
                    next_val = comma_pattern.sub(r"\1\2", current)
                    if next_val == current:
                        break
                    current = next_val
                
                if current != original:
                    cleaned[field] = current
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": current,
                        "rule_code": self.rule_code
                    })
                    
        return cleaned, corrections
