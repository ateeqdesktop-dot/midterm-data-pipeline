from typing import Dict, Any, List, Tuple
from config import settings
from src.cleaning.base_rule import CleaningRule

class CurrencyExchangeRule(CleaningRule):
    """
    Rule 15: Converts foreign currencies (USD, SAR) to standard target currency (YER)
    using business exchange rates in settings.EXCHANGE_RATES, and normalizes 'currency' to TARGET_CURRENCY.
    """
    @property
    def rule_code(self) -> str:
        return "CURRENCY_EXCHANGE_CONVERSION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        currency = cleaned.get("currency")
        target_currency = getattr(settings, "TARGET_CURRENCY", "YER")
        exchange_rates = getattr(settings, "EXCHANGE_RATES", {"YER": 1.0, "USD": 600.0, "SAR": 160.0})
        
        # Only convert if currency is defined, is not target currency, and is in exchange rates
        if currency and currency != target_currency and currency in exchange_rates:
            rate = exchange_rates[currency]
            
            # Convert numeric fields
            amount_fields = ["total_amount", "payment_amount", "delivery_cost"]
            for field in amount_fields:
                val = cleaned.get(field)
                if val is not None:
                    try:
                        orig_num = float(str(val).strip())
                        converted_num = round(orig_num * rate, 2)
                        cleaned[field] = str(converted_num)
                        corrections.append({
                            "field": field,
                            "original_value": val,
                            "corrected_value": str(converted_num),
                            "rule_code": f"{self.rule_code}_{currency}_TO_{target_currency}"
                        })
                    except (ValueError, TypeError):
                        pass
                        
            # Normalize currency to target
            cleaned["currency"] = target_currency
            corrections.append({
                "field": "currency",
                "original_value": currency,
                "corrected_value": target_currency,
                "rule_code": self.rule_code
            })
            
        return cleaned, corrections
