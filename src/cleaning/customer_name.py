import re
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class CustomerNameNormalizeRule(CleaningRule):
    """
    Rule 11: Cleans and standardizes customer_name:
    - Removes honorific titles/prefixes (المهندس, الدكتور, د., م., الاستاذ, السيد, etc.)
    - Removes accidental trailing numbers or special punctuation symbols
    - Normalizes multiple spaces and removes tatweel (ـ)
    """
    @property
    def rule_code(self) -> str:
        return "CUSTOMER_NAME_NORMALIZATION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        field = "customer_name"
        val = cleaned.get(field)
        
        if isinstance(val, str) and val.strip():
            original = val
            name = val.strip()
            
            # Remove tatweel, zero-width chars
            name = re.sub(r"[\u200B-\u200D\uFEFF\u0640]", "", name)
            
            # Remove honorific prefixes at start of name
            honorifics = [
                r"^(المهندس|المهندسة|مهندس|مهندسة)\s+",
                r"^(الدكتور|الدكتورة|دكتور|دكتورة|د\.)\s+",
                r"^(الاستاذ|الأستاذ|الاستاذة|الأستاذة|استاذ|أستاذ|أ\.)\s+",
                r"^(الشيخ|الشيخة|شيخ|شيخة)\s+",
                r"^(السيد|السيدة|سيد|سيدة)\s+",
                r"^(الأخ|الاخ|الأخت|الاخت)\s+"
            ]
            for pat in honorifics:
                name = re.sub(pat, "", name, flags=re.IGNORECASE).strip()
                
            # Remove trailing numbers or symbols (e.g. "محمد علي 123" or "محمد علي #")
            name = re.sub(r"[\d#@!$%^&*()_+=\[\]{};:\"'<>?]+$", "", name).strip()
            
            # Remove extra internal spaces
            name = re.sub(r"\s+", " ", name).strip()
            
            if name != original and len(name) >= 2:
                cleaned[field] = name
                corrections.append({
                    "field": field,
                    "original_value": original,
                    "corrected_value": name,
                    "rule_code": self.rule_code
                })
                
        return cleaned, corrections
