import json
from src.cleaning.arabic_numbers import ArabicNumbersRule
from src.cleaning.currency_text import CurrencyTextRule
from src.cleaning.thousands_sep import ThousandsSeparatorRule
from src.cleaning.word_price import WordPriceRule
from src.cleaning.phone_number import PhoneNormalizationRule
from src.cleaning.email_fix import EmailFixRule
from src.cleaning.date_normalize import DateNormalizeRule
from src.cleaning.status_normalize import StatusNormalizeRule
from src.cleaning.total_recalc import TotalRecalculationRule

def test_arabic_numbers_rule():
    rule = ArabicNumbersRule()
    record = {
        "total_amount": "٧٠٦٠٠٠٫٠",
        "payment_amount": "123000",
        "delivery_cost": "٢٠٠٠٫٠",
        "customer_phone": "٧٧١٢٣٤٥٦٧"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["total_amount"] == "706000.0"
    assert cleaned["delivery_cost"] == "2000.0"
    assert cleaned["customer_phone"] == "771234567"
    assert len(corrections) == 3
    assert corrections[0]["field"] == "total_amount"
    assert corrections[0]["rule_code"] == "ARABIC_NUMERALS"

def test_currency_text_rule():
    rule = CurrencyTextRule()
    record = {
        "total_amount": "54000.00 ريال",
        "payment_amount": "54000.00 ريال يمني",
        "currency": "YER"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["total_amount"] == "54000.00"
    assert cleaned["payment_amount"] == "54000.00"
    
    # Mismatched currency
    record2 = {
        "total_amount": "54000.00 ريال",
        "currency": "UNKNOWN"
    }
    cleaned2, corrections2 = rule.apply(record2)
    assert cleaned2["currency"] == "YER"
    assert len(corrections2) == 2

def test_thousands_separator_rule():
    rule = ThousandsSeparatorRule()
    record = {
        "total_amount": "125,000.00",
        "payment_amount": "1,234,567.89"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["total_amount"] == "125000.00"
    assert cleaned["payment_amount"] == "1234567.89"
    assert len(corrections) == 2

def test_word_price_rule():
    rule = WordPriceRule()
    record = {
        "total_amount": "ألفان ريال",
        "payment_amount": "خمسة آلاف"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["total_amount"] == "2000"
    assert cleaned["payment_amount"] == "5000"

def test_phone_normalization_rule():
    rule = PhoneNormalizationRule()
    # 1. Reverse order phone
    record = {"customer_phone": "4567 123 77 967+"}
    cleaned, corrections = rule.apply(record)
    assert cleaned["customer_phone"] == "+967771234567"
    
    # 2. Local number
    record2 = {"customer_phone": "702390941"}
    cleaned2, corrections2 = rule.apply(record2)
    assert cleaned2["customer_phone"] == "+967702390941"

def test_email_fix_rule():
    rule = EmailFixRule()
    record = {"customer_email": "user@@mail..com"}
    cleaned, corrections = rule.apply(record)
    assert cleaned["customer_email"] == "user@mail.com"

def test_date_normalize_rule():
    rule = DateNormalizeRule()
    # DD/MM/YYYY
    record = {"order_date": "31/01/2025"}
    cleaned, corrections = rule.apply(record)
    assert cleaned["order_date"] == "2025-01-31T00:00:00"
    
    # ISO but spaces
    record2 = {"order_date": " 2025-02-24T21:29:00 "}
    cleaned2, corrections2 = rule.apply(record2)
    assert cleaned2["order_date"] == "2025-02-24T21:29:00"

def test_status_normalize_rule():
    rule = StatusNormalizeRule()
    record = {
        "status": "بانتظار ",
        "payment_status": "تم الدفع",
        "customer_name": "  محمد علي  "
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["status"] == "قيد الانتظار"
    assert cleaned["customer_name"] == "محمد علي"

def test_total_recalculation_rule():
    rule = TotalRecalculationRule()
    # Negative quantity corrected, and total recalculation matches
    items = [
        {"sku": "SKU-1010", "name": "Phone", "qty": -2, "unit_price": 100.0, "total": 300.0}, # unit*abs(qty)=200, total says 300, which is incorrect
        {"sku": "SKU-1002", "name": "Mouse", "qty": 1, "unit_price": 50.0, "total": 50.0}
    ]
    record = {
        "items_json": json.dumps(items),
        "delivery_cost": "10",
        "total_amount": "300"
    }
    cleaned, corrections = rule.apply(record)
    
    # Item 1 qty is corrected to 2.
    # Item 1 total is corrected to 200.0 (2 * 100.0).
    # Expected sum = 200.0 + 50.0 = 250.0.
    # Expected total_amount = 250.0 + 10.0 = 260.0.
    assert cleaned["total_amount"] == "260.0"
    parsed_items = json.loads(cleaned["items_json"])
    assert parsed_items[0]["qty"] == 2
    assert parsed_items[0]["total"] == 200.0
