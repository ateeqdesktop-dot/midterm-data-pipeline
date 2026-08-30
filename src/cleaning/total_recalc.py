import json
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class TotalRecalculationRule(CleaningRule):
    """
    Rule 9: Parses items_json, normalizes negative quantities where item total is positive,
    recalculates sum of items + delivery_cost, and corrects total_amount / payment_amount if they mismatch.
    """
    @property
    def rule_code(self) -> str:
        return "TOTAL_RECALCULATION"
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        items_raw = cleaned.get("items_json")
        delivery_cost_val = cleaned.get("delivery_cost", "0.0")
        
        # Parse delivery cost
        try:
            delivery_cost = float(delivery_cost_val) if delivery_cost_val else 0.0
        except ValueError:
            delivery_cost = 0.0
            
        if isinstance(items_raw, str) and items_raw.strip():
            try:
                items = json.loads(items_raw)
                if not isinstance(items, list):
                    return cleaned, corrections
                    
                items_changed = False
                items_total_sum = 0.0
                
                for idx, item in enumerate(items):
                    def clean_numeric(val):
                        if isinstance(val, (int, float)):
                            return float(val)
                        if not isinstance(val, str):
                            return 0.0
                        arabic_digits = "٠١٢٣٤٥٦٧٨٩٫"
                        latin_digits = "0123456789."
                        trans_table = str.maketrans(arabic_digits, latin_digits)
                        cleaned_str = val.translate(trans_table).replace(",", "").strip()
                        try:
                            return float(cleaned_str)
                        except ValueError:
                            return 0.0

                    qty = clean_numeric(item.get("qty", 0))
                    unit_price = clean_numeric(item.get("unit_price", 0.0))
                    total = clean_numeric(item.get("total", 0.0))
                    
                    # Fix negative quantity if the total is positive and matches absolute quantity
                    if qty < 0 and total > 0:
                        original_qty = item.get("qty")
                        qty = abs(qty)
                        item["qty"] = qty
                        items_changed = True
                        corrections.append({
                            "field": f"items_json[{idx}].qty",
                            "original_value": original_qty,
                            "corrected_value": qty,
                            "rule_code": "NEGATIVE_QTY_CORRECTION"
                        })
                        
                    # Calculate item total if it is incorrect or negative but qty and unit_price are positive
                    calculated_total = qty * unit_price
                    if abs(calculated_total - total) > 0.01 and qty > 0 and unit_price > 0:
                        original_total = item.get("total")
                        total = calculated_total
                        item["total"] = total
                        items_changed = True
                        corrections.append({
                            "field": f"items_json[{idx}].total",
                            "original_value": original_total,
                            "corrected_value": total,
                            "rule_code": "ITEM_TOTAL_RECALCULATION"
                        })
                        
                    items_total_sum += total
                    
                # If we modified items inside the list, dump back to string
                if items_changed:
                    cleaned["items_json"] = json.dumps(items, ensure_ascii=False)
                    
                # Re-verify and correct total_amount
                expected_total = items_total_sum + delivery_cost
                total_amount_val = cleaned.get("total_amount")
                
                try:
                    total_amount = float(total_amount_val) if total_amount_val else 0.0
                except ValueError:
                    total_amount = 0.0
                    
                # Allow a small tolerance for float comparison (e.g. 0.01 YER)
                if abs(total_amount - expected_total) > 0.01:
                    cleaned["total_amount"] = str(expected_total)
                    corrections.append({
                        "field": "total_amount",
                        "original_value": total_amount_val,
                        "corrected_value": str(expected_total),
                        "rule_code": self.rule_code
                    })
                    
                # Also correct payment_amount if payment_status is "تم الدفع" and it mismatches total_amount
                payment_status = cleaned.get("payment_status")
                payment_amount_val = cleaned.get("payment_amount")
                try:
                    payment_amount = float(payment_amount_val) if payment_amount_val else 0.0
                except ValueError:
                    payment_amount = 0.0
                    
                if payment_status == "تم الدفع" and abs(payment_amount - expected_total) > 0.01:
                    cleaned["payment_amount"] = str(expected_total)
                    corrections.append({
                        "field": "payment_amount",
                        "original_value": payment_amount_val,
                        "corrected_value": str(expected_total),
                        "rule_code": "PAYMENT_AMOUNT_RECALC"
                    })
                    
            except Exception as json_err:
                # If json is corrupted, we don't raise error here, we let Quarantine handle it
                pass
                
        return cleaned, corrections
