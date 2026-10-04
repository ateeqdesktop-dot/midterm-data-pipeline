from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule
from src.cleaning.status_normalize import StatusNormalizeRule
from src.cleaning.customer_name import CustomerNameNormalizeRule
from src.cleaning.arabic_numbers import ArabicNumbersRule
from src.cleaning.thousands_sep import ThousandsSeparatorRule
from src.cleaning.currency_text import CurrencyTextRule
from src.cleaning.word_price import WordPriceRule
from src.cleaning.date_normalize import DateNormalizeRule
from src.cleaning.phone_number import PhoneNormalizationRule
from src.cleaning.email_fix import EmailFixRule
from src.cleaning.city_normalize import CityNormalizeRule
from src.cleaning.district_normalize import DistrictNormalizeRule
from src.cleaning.delivery_type import DeliveryTypeNormalizeRule
from src.cleaning.payment_method import PaymentMethodNormalizeRule
from src.cleaning.total_recalc import TotalRecalculationRule
from src.cleaning.currency_exchange import CurrencyExchangeRule

class RuleRegistry:
    """
    Registers and executes all cleaning rules sequentially in optimal order.
    """
    def __init__(self):
        # Optimal sequence of execution:
        self.rules: List[CleaningRule] = [
            StatusNormalizeRule(),
            CustomerNameNormalizeRule(),
            ArabicNumbersRule(),
            ThousandsSeparatorRule(),
            CurrencyTextRule(),
            WordPriceRule(),
            DateNormalizeRule(),
            PhoneNormalizationRule(),
            EmailFixRule(),
            CityNormalizeRule(),
            DistrictNormalizeRule(),
            DeliveryTypeNormalizeRule(),
            PaymentMethodNormalizeRule(),
            TotalRecalculationRule(),
            CurrencyExchangeRule()
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


