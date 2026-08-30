from typing import Dict, Any, List, Tuple
from src.cleaning.arabic_numbers import ArabicNumbersRule
from src.cleaning.currency_text import CurrencyTextRule
from src.cleaning.thousands_sep import ThousandsSeparatorRule
from src.cleaning.word_price import WordPriceRule
from src.cleaning.phone_number import PhoneNormalizationRule
from src.cleaning.email_fix import EmailFixRule
from src.cleaning.date_normalize import DateNormalizeRule
from src.cleaning.status_normalize import StatusNormalizeRule
from src.cleaning.total_recalc import TotalRecalculationRule

class RuleRegistry:
    """
    Registers and executes all cleaning rules sequentially.
    """
    def __init__(self):
        # Register rules in logical sequence: text translations first, then number formatters, then business logic.
        self.rules = [
            ArabicNumbersRule(),
            ThousandsSeparatorRule(),
            CurrencyTextRule(),
            WordPriceRule(),
            PhoneNormalizationRule(),
            EmailFixRule(),
            DateNormalizeRule(),
            StatusNormalizeRule(),
            TotalRecalculationRule()
        ]
        
    def clean_record(self, raw_record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Runs all rules on a record and collects audit trail logs.
        """
        cleaned = raw_record.copy()
        all_corrections = []
        
        for rule in self.rules:
            try:
                cleaned, corrections = rule.apply(cleaned)
                all_corrections.extend(corrections)
            except Exception as e:
                print(f"Error applying rule {rule.rule_code}: {e}")
                # We do not crash the pipeline; log it and proceed with other rules
                
        return cleaned, all_corrections
