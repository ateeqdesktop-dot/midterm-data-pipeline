import json
from src.cleaning.arabic_numbers import ArabicNumbersRule
from src.cleaning.currency_text import CurrencyTextRule
from src.cleaning.thousands_sep import ThousandsSeparatorRule
from src.cleaning.word_price import WordPriceRule
from src.cleaning.phone_number import PhoneNormalizationRule
from src.cleaning.email_fix import EmailFixRule
from src.cleaning.city_normalize import CityNormalizeRule
from src.cleaning.date_normalize import DateNormalizeRule
from src.cleaning.status_normalize import StatusNormalizeRule
from src.cleaning.total_recalc import TotalRecalculationRule

def test_arabic_numbers_rule():
    rule = ArabicNumbersRule()
    record = {
        "order_id": "طلب-١٠٠٠١٧",
        "total_amount": "٧٠٦٠٠٠٫٠",
        "payment_amount": "123000",
        "delivery_cost": "٢٠٠٠٫٠",
        "customer_phone": "٧٧١٢٣٤٥٦٧",
        "order_date": "٢٠٢٥-٠١-١٧ ٠٤:٥٠:٠٠",
        "items_json": '[{"qty": ٣, "unit_price": ٤٣٥٠٠}]'
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["order_id"] == "طلب-100017"
    assert cleaned["total_amount"] == "706000.0"
    assert cleaned["delivery_cost"] == "2000.0"
    assert cleaned["customer_phone"] == "771234567"
    assert cleaned["order_date"] == "2025-01-17 04:50:00"
    assert '{"qty": 3, "unit_price": 43500}' in cleaned["items_json"]
    assert len(corrections) == 6

def test_currency_text_rule():
    rule = CurrencyTextRule()
    record = {
        "total_amount": "54000.00 ريال",
        "payment_amount": "54000.00 ريال يمني",
        "currency": "ريال يمني"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["total_amount"] == "54000.00"
    assert cleaned["payment_amount"] == "54000.00"
    assert cleaned["currency"] == "YER"
    
    # Currency symbols embedded with USD
    record2 = {
        "total_amount": "$ 540.00",
        "currency": ""
    }
    cleaned2, corrections2 = rule.apply(record2)
    assert cleaned2["total_amount"] == "540.00"
    assert cleaned2["currency"] == "USD"

def test_thousands_separator_rule():
    rule = ThousandsSeparatorRule()
    record = {
        "total_amount": "125,000.00",
        "payment_amount": "1,234,567.89",
        "delivery_cost": "5 000.00"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["total_amount"] == "125000.00"
    assert cleaned["payment_amount"] == "1234567.89"
    assert cleaned["delivery_cost"] == "5000.00"
    assert len(corrections) == 3

def test_word_price_rule():
    rule = WordPriceRule()
    record = {
        "total_amount": "ألفان ريال",
        "payment_amount": "خمسة آلاف وخمسمائة",
        "delivery_cost": "ألف وخمسمائة ريال يمني"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["total_amount"] == "2000"
    assert cleaned["payment_amount"] == "5500"
    assert cleaned["delivery_cost"] == "1500"

def test_phone_normalization_rule():
    rule = PhoneNormalizationRule()
    # 1. Reverse order phone
    record = {"customer_phone": "4567 123 77 967+"}
    cleaned, corrections = rule.apply(record)
    assert cleaned["customer_phone"] == "+967771234567"
    
    # 2. Local 9-digit mobile number
    record2 = {"customer_phone": "702390941"}
    cleaned2, corrections2 = rule.apply(record2)
    assert cleaned2["customer_phone"] == "+967702390941"
    
    # 3. 07 prefix mobile number
    record3 = {"customer_phone": "0734577257"}
    cleaned3, corrections3 = rule.apply(record3)
    assert cleaned3["customer_phone"] == "+967734577257"
    
    # 4. 00967 prefix
    record4 = {"customer_phone": "00967716489644"}
    cleaned4, corrections4 = rule.apply(record4)
    assert cleaned4["customer_phone"] == "+967716489644"

def test_email_fix_rule():
    rule = EmailFixRule()
    record = {"customer_email": "<user@@mail..com>"}
    cleaned, corrections = rule.apply(record)
    assert cleaned["customer_email"] == "user@mail.com"
    
    # Comma in domain
    record2 = {"customer_email": "user @ example,com"}
    cleaned2, corrections2 = rule.apply(record2)
    assert cleaned2["customer_email"] == "user@example.com"

def test_city_normalize_rule():
    rule = CityNormalizeRule()
    record = {"city": "صنعا"}
    cleaned, corrections = rule.apply(record)
    assert cleaned["city"] == "صنعاء"
    
    record2 = {"city": "الحديده"}
    cleaned2, corrections2 = rule.apply(record2)
    assert cleaned2["city"] == "الحديدة"

def test_date_normalize_rule():
    rule = DateNormalizeRule()
    # DD-MM-YYYY HH:MM:SS
    record1 = {"order_date": "17-01-2025 04:50:00"}
    cleaned1, _ = rule.apply(record1)
    assert cleaned1["order_date"] == "2025-01-17T04:50:00"
    
    # YYYY/MM/DD HH:MM:SS
    record2 = {"order_date": "2025/04/11 13:41:00"}
    cleaned2, _ = rule.apply(record2)
    assert cleaned2["order_date"] == "2025-04-11T13:41:00"
    
    # DD/MM/YYYY
    record3 = {"order_date": "31/01/2025"}
    cleaned3, _ = rule.apply(record3)
    assert cleaned3["order_date"] == "2025-01-31T00:00:00"
    
    # Arabic AM/PM
    record4 = {"order_date": "2025-01-17 04:50:00 ص"}
    cleaned4, _ = rule.apply(record4)
    assert cleaned4["order_date"] == "2025-01-17T04:50:00"

def test_status_normalize_rule():
    rule = StatusNormalizeRule()
    record = {
        "status": "  تم التسليم  ",
        "payment_status": "قيد الدفع",
        "customer_name": "  محمد علي  "
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["status"] == "توصيل"
    assert cleaned["payment_status"] == "بانتظار الدفع"
    assert cleaned["customer_name"] == "محمد علي"

def test_total_recalculation_rule():
    rule = TotalRecalculationRule()
    # Negative quantity corrected, and total recalculation matches
    items = [
        {"sku": "SKU-1010", "name": "Phone", "qty": -2, "unit_price": 100.0, "total": 300.0},
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

def test_total_recalculation_missing_price_inference():
    rule = TotalRecalculationRule()
    # Missing / unknown price '???' recovered from items_json
    items = [
        {"sku": "SKU-1005", "name": "Headphones", "qty": 3, "unit_price": 43500.0, "total": 130500.0}
    ]
    record = {
        "items_json": json.dumps(items),
        "delivery_cost": "2000.0",
        "total_amount": "???",
        "payment_status": "تم الدفع",
        "payment_amount": "???"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["total_amount"] == "132500.0"
    assert cleaned["payment_amount"] == "132500.0"

def test_customer_name_normalize_rule():
    from src.cleaning.customer_name import CustomerNameNormalizeRule
    rule = CustomerNameNormalizeRule()
    
    # 1. Honorific title removal
    record = {"customer_name": "المهندس محمد علي"}
    cleaned, _ = rule.apply(record)
    assert cleaned["customer_name"] == "محمد علي"
    
    # 2. Doctor title with dot and tatweel
    record2 = {"customer_name": "د. أحـمـد سـالـم"}
    cleaned2, _ = rule.apply(record2)
    assert cleaned2["customer_name"] == "أحمد سالم"

def test_district_normalize_rule():
    from src.cleaning.district_normalize import DistrictNormalizeRule
    rule = DistrictNormalizeRule()
    
    # Dictionary mapping
    record = {"district": "حده"}
    cleaned, _ = rule.apply(record)
    assert cleaned["district"] == "حدة"
    
    # Fuzzy matching
    record2 = {"district": "القاهره"}
    cleaned2, _ = rule.apply(record2)
    assert cleaned2["district"] == "القاهرة"

def test_delivery_type_normalize_rule():
    from src.cleaning.delivery_type import DeliveryTypeNormalizeRule
    rule = DeliveryTypeNormalizeRule()
    
    record = {"delivery_type": "توصيل سريع"}
    cleaned, _ = rule.apply(record)
    assert cleaned["delivery_type"] == "سريع"
    
    record2 = {"delivery_type": "express"}
    cleaned2, _ = rule.apply(record2)
    assert cleaned2["delivery_type"] == "سريع"

def test_payment_method_normalize_rule():
    from src.cleaning.payment_method import PaymentMethodNormalizeRule
    rule = PaymentMethodNormalizeRule()
    
    record = {"payment_method": "كاش"}
    cleaned, _ = rule.apply(record)
    assert cleaned["payment_method"] == "نقد عند الاستلام"
    
    record2 = {"payment_method": "محفظة"}
    cleaned2, _ = rule.apply(record2)
    assert cleaned2["payment_method"] == "محفظة إلكترونية"

def test_currency_exchange_rule():
    from src.cleaning.currency_exchange import CurrencyExchangeRule
    rule = CurrencyExchangeRule()
    
    # USD to YER conversion (rate 600.0)
    record = {
        "currency": "USD",
        "total_amount": "100.0",
        "payment_amount": "100.0",
        "delivery_cost": "5.0"
    }
    cleaned, corrections = rule.apply(record)
    assert cleaned["currency"] == "YER"
    assert cleaned["total_amount"] == "60000.0"
    assert cleaned["payment_amount"] == "60000.0"
    assert cleaned["delivery_cost"] == "3000.0"


