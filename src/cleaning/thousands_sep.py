import re
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class ThousandsSeparatorRule(CleaningRule):
    """
    Rule 3: Removes thousands separators (commas, Arabic commas '،', and spaces) from numeric fields.
    Example: "125,000.00" -> "125000.00", "1 234 567.89" -> "1234567.89"
    """
    @property
    def rule_code(self) -> str:
        return "THOUSANDS_SEPARATOR"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        numeric_fields = ["total_amount", "payment_amount", "delivery_cost"]
        
        # Patterns for thousands separators:
        # 1. Comma / Arabic comma between digits: 1,000 or 1،000
        comma_sep_pattern = re.compile(r"(\d)[,،](\d{3})")
        # 2. Space between digits where next group has 3 digits: 1 000
        space_sep_pattern = re.compile(r"(\d)\s+(\d{3})(?=\D|$)")
        
        for field in numeric_fields:
            val = cleaned.get(field)
            if isinstance(val, str) and ("," in val or "،" in val or " " in val):
                original = val
                current = val
                
                # Replace comma and arabic comma separators iteratively
                while True:
                    next_val = comma_sep_pattern.sub(r"\1\2", current)
                    if next_val == current:
                        break
                    current = next_val
                    
                # Replace space separators iteratively
                while True:
                    next_val = space_sep_pattern.sub(r"\1\2", current)
                    if next_val == current:
                        break
                    current = next_val
                    
                # If there are still lone commas in a purely numeric-like string (e.g. 5000,00 -> 5000.00 if decimal)
                # Check if only one comma remains with 2 digits at end: convert to decimal dot
                if re.match(r"^\d+,(\d{1,2})$", current):
                    current = current.replace(",", ".")
                
                if current != original:
                    cleaned[field] = current.strip()
                    corrections.append({
                        "field": field,
                        "original_value": original,
                        "corrected_value": current.strip(),
                        "rule_code": self.rule_code
                    })
                    
        return cleaned, corrections

