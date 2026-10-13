import json
from datetime import datetime
from typing import Dict, Any, List, Set, Tuple, Optional
from config import settings

def classify_quarantine_errors(
    record: Dict[str, Any], 
    seen_order_ids: Optional[Set[str]] = None
) -> Tuple[List[str], List[str]]:
    """
    Validates a record against the 9 quarantine requirements of Section 6.8 of the spec:
    1. MISSING_ORDER_ID: order_id is missing, null, or empty string.
    2. MISSING_CUSTOMER_ID: customer_id is missing, null, or empty string.
    3. INVALID_IMPOSSIBLE_DATE: order_date is missing, invalid, or impossible date.
    4. CORRUPTED_ITEMS_JSON: items_json is corrupted, null, or invalid JSON structure.
    5. EMPTY_ITEMS: items_json parsed to empty list.
    6. UNKNOWN_PRICE: total_amount is missing or cannot be inferred.
    7. AMBIGUOUS_NEGATIVE_VALUE: negative quantity, delivery cost, or price.
    8. DUPLICATE_ORDER_ID: duplicate order_id seen in this run.
    9. MULTIPLE_CONFLICTING_ERRORS: multiple distinct fatal validation errors.

    Returns:
        Tuple[error_codes, error_details]
    """
    if seen_order_ids is None:
        seen_order_ids = set()

    error_codes: List[str] = []
    error_details: List[str] = []
    
    # 1. order_id Check
    order_id = record.get("order_id")
    if order_id is None:
        error_codes.append("MISSING_ORDER_ID")
        error_details.append("Order ID is missing (null).")
    else:
        order_id_str = str(order_id).strip()
        if not order_id_str or order_id_str.lower() in ("null", "none", "nan", "n/a", "undefined", "???", "?"):
            error_codes.append("MISSING_ORDER_ID")
            error_details.append(f"Order ID is empty or placeholder ('{order_id_str}').")
        elif order_id_str in seen_order_ids:
            error_codes.append("DUPLICATE_ORDER_ID")
            error_details.append(f"Duplicate Order ID '{order_id_str}' encountered in this run.")
            
    # 2. customer_id Check
    customer_id = record.get("customer_id")
    if customer_id is None:
        error_codes.append("MISSING_CUSTOMER_ID")
        error_details.append("Customer ID is missing (null).")
    else:
        customer_id_str = str(customer_id).strip()
        if not customer_id_str or customer_id_str.lower() in ("null", "none", "nan", "n/a", "undefined", "???", "?"):
            error_codes.append("MISSING_CUSTOMER_ID")
            error_details.append(f"Customer ID is empty or placeholder ('{customer_id_str}').")
        
    # 3. order_date Check
    order_date = record.get("order_date")
    if not order_date or str(order_date).strip().lower() in ("", "null", "none", "nan", "invalid", "???", "invalid-date"):
        error_codes.append("INVALID_IMPOSSIBLE_DATE")
        error_details.append("Order Date is missing or invalid placeholder.")
    else:
        date_str = str(order_date).strip()
        parsed_dt = None
        # Try standard formats: ISO with T, ISO with space, date only
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d-%m-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S"
        ):
            try:
                parsed_dt = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
                
        if parsed_dt is None:
            error_codes.append("INVALID_IMPOSSIBLE_DATE")
            error_details.append(f"Order Date '{date_str}' is invalid or unparseable.")
        elif parsed_dt.year < 2000 or parsed_dt.year > 2035:
            error_codes.append("INVALID_IMPOSSIBLE_DATE")
            error_details.append(f"Order Date year {parsed_dt.year} is outside plausible range (2000-2035).")
            
    # 4. items_json Check
    items_raw = record.get("items_json")
    items = None
    if not items_raw or str(items_raw).strip().lower() in ("", "null", "none", "nan", "not-json"):
        error_codes.append("CORRUPTED_ITEMS_JSON")
        error_details.append("Items JSON is missing, null, or empty string.")
    else:
        try:
            if isinstance(items_raw, str):
                items = json.loads(items_raw)
            elif isinstance(items_raw, list):
                items = items_raw
            else:
                items = None
                
            if not isinstance(items, list):
                error_codes.append("CORRUPTED_ITEMS_JSON")
                error_details.append("Items JSON is not a list structure.")
            elif len(items) == 0:
                error_codes.append("EMPTY_ITEMS")
                error_details.append("Order has an empty items list.")
            else:
                for item in items:
                    if not isinstance(item, dict):
                        error_codes.append("CORRUPTED_ITEMS_JSON")
                        error_details.append("Item in items_json is not an object.")
                        break
        except Exception as e:
            error_codes.append("CORRUPTED_ITEMS_JSON")
            error_details.append(f"Items JSON cannot be parsed: {e}")
            
    # 5. Price / total_amount Check
    total_amount_raw = record.get("total_amount")
    total_amount = None
    if total_amount_raw is None or str(total_amount_raw).strip().lower() in ("", "null", "none", "nan", "???", "?", "غير معروف"):
        error_codes.append("UNKNOWN_PRICE")
        error_details.append("Total amount is missing or undefined.")
    else:
        try:
            clean_tot = str(total_amount_raw).replace(",", "").strip()
            total_amount = float(clean_tot)
        except (ValueError, TypeError):
            error_codes.append("UNKNOWN_PRICE")
            error_details.append(f"Total amount '{total_amount_raw}' is not a valid number.")
        
    # 6. Negative values Check (AMBIGUOUS_NEGATIVE_VALUE)
    delivery_cost_raw = record.get("delivery_cost", "0.0")
    try:
        if delivery_cost_raw is not None and str(delivery_cost_raw).strip():
            delivery_cost = float(str(delivery_cost_raw).replace(",", "").strip())
            if delivery_cost < 0:
                error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
                error_details.append(f"Delivery cost '{delivery_cost}' is negative.")
    except (ValueError, TypeError):
        pass
        
    if total_amount is not None and total_amount < 0:
        error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
        error_details.append(f"Total amount '{total_amount}' is negative.")
        
    payment_amount_raw = record.get("payment_amount")
    try:
        if payment_amount_raw is not None and str(payment_amount_raw).strip():
            payment_amount = float(str(payment_amount_raw).replace(",", "").strip())
            if payment_amount < 0:
                error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
                error_details.append(f"Payment amount '{payment_amount}' is negative.")
    except (ValueError, TypeError):
        pass
        
    if items and isinstance(items, list):
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            try:
                qty = float(item.get("qty", 0))
            except (ValueError, TypeError):
                qty = 0.0
            try:
                unit_price = float(item.get("unit_price", 0.0))
            except (ValueError, TypeError):
                unit_price = 0.0
            try:
                total = float(item.get("total", 0.0))
            except (ValueError, TypeError):
                total = 0.0
                
            if qty < 0 or unit_price < 0 or total < 0:
                error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
                error_details.append(
                    f"Item at index {idx} has negative value(s): qty={qty}, unit_price={unit_price}, total={total}"
                )
                break
                
    # 7. Check for multiple conflicting errors (section 6.8)
    distinct_errors = set(error_codes)
    if len(distinct_errors) >= 2:
        error_codes.append("MULTIPLE_CONFLICTING_ERRORS")
        error_details.append("Record contains multiple distinct validation errors preventing safe resolution.")
        
    # Deduplicate error_codes while preserving order
    dedup_codes = list(dict.fromkeys(error_codes))
    return dedup_codes, error_details
