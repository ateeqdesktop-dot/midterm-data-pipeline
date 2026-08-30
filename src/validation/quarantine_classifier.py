import json
from datetime import datetime
from typing import Dict, Any, List, Set, Tuple
from config import settings

def classify_quarantine_errors(record: Dict[str, Any], seen_order_ids: Set[str]) -> Tuple[List[str], List[str]]:
    """
    Validates a cleaned record.
    Returns:
        Tuple[error_codes, error_details]
    """
    error_codes = []
    error_details = []
    
    # 1. order_id Check
    order_id = record.get("order_id")
    if not order_id:
        error_codes.append("MISSING_ORDER_ID")
        error_details.append("Order ID is missing, null, or empty.")
    else:
        order_id_str = str(order_id).strip()
        if not order_id_str:
            error_codes.append("MISSING_ORDER_ID")
            error_details.append("Order ID is empty string.")
        elif order_id_str in seen_order_ids:
            error_codes.append("DUPLICATE_ORDER_ID")
            error_details.append(f"Duplicate Order ID '{order_id_str}' encountered in this run.")
            
    # 2. customer_id Check
    customer_id = record.get("customer_id")
    if not customer_id or not str(customer_id).strip():
        error_codes.append("MISSING_CUSTOMER_ID")
        error_details.append("Customer ID is missing or empty.")
        
    # 3. order_date Check
    order_date = record.get("order_date")
    if not order_date:
        error_codes.append("INVALID_IMPOSSIBLE_DATE")
        error_details.append("Order Date is missing.")
    else:
        # Check if date is in standardized ISO format YYYY-MM-DDTHH:MM:SS
        try:
            datetime.strptime(str(order_date), "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            error_codes.append("INVALID_IMPOSSIBLE_DATE")
            error_details.append(f"Order Date '{order_date}' is invalid or not standardized.")
            
    # 4. items_json Check
    items_raw = record.get("items_json")
    items = None
    if not items_raw:
        error_codes.append("CORRUPTED_ITEMS_JSON")
        error_details.append("Items JSON is missing or null.")
    else:
        try:
            items = json.loads(items_raw)
            if not isinstance(items, list):
                error_codes.append("CORRUPTED_ITEMS_JSON")
                error_details.append("Items JSON is not a list structure.")
            elif len(items) == 0:
                error_codes.append("EMPTY_ITEMS")
                error_details.append("Order has an empty items list.")
        except Exception as e:
            error_codes.append("CORRUPTED_ITEMS_JSON")
            error_details.append(f"Items JSON cannot be parsed: {e}")
            
    # 5. Price / total_amount Check
    total_amount_raw = record.get("total_amount")
    total_amount = None
    try:
        if total_amount_raw is not None:
            total_amount = float(total_amount_raw)
        else:
            error_codes.append("UNKNOWN_PRICE")
            error_details.append("Total amount is missing.")
    except ValueError:
        error_codes.append("UNKNOWN_PRICE")
        error_details.append(f"Total amount '{total_amount_raw}' is not a valid number.")
        
    # 6. Negative values Check
    delivery_cost_raw = record.get("delivery_cost", "0.0")
    try:
        delivery_cost = float(delivery_cost_raw) if delivery_cost_raw else 0.0
        if delivery_cost < 0:
            error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
            error_details.append(f"Delivery cost '{delivery_cost}' is negative.")
    except ValueError:
        pass
        
    if total_amount is not None and total_amount < 0:
        error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
        error_details.append(f"Total amount '{total_amount}' is negative.")
        
    if items and isinstance(items, list):
        for idx, item in enumerate(items):
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
                
    # 7. Check for multiple conflicting errors (section 6.8: multiple conflicting errors code)
    # If we have 3 or more distinct errors, flag it as multiple conflicting errors
    if len(error_codes) >= 3:
        error_codes.append("MULTIPLE_CONFLICTING_ERRORS")
        error_details.append("Record contains three or more distinct validation errors, preventing safe resolution.")
        
    return error_codes, error_details
