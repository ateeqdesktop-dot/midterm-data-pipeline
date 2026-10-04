import json
import re
import ast
from typing import Dict, Any, List, Tuple
from src.cleaning.base_rule import CleaningRule

class TotalRecalculationRule(CleaningRule):
    """
    Rule 9: Parses items_json, normalizes negative quantities where item total is positive,
    repairs minor JSON syntax issues, recalculates sum of items + delivery_cost,
    and reliably recovers total_amount / payment_amount when unknown or missing.
    """
    @property
    def rule_code(self) -> str:
        return "TOTAL_RECALCULATION"
        
    def _try_parse_items_json(self, raw_str: str) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Attempts to parse items_json string with safe repair heuristics.
        Returns (parsed_items_list, was_repaired).
        """
        if not raw_str or not isinstance(raw_str, str):
            return [], False
            
        cleaned_str = raw_str.strip()
        
        # 1. Standard JSON parse attempt
        try:
            parsed = json.loads(cleaned_str)
            if isinstance(parsed, list):
                return parsed, False
        except Exception:
            pass
            
        # 2. Repair: Strip outer quotes if doubly quoted: '"[{...}]"'
        if cleaned_str.startswith('"') and cleaned_str.endswith('"') and len(cleaned_str) > 2:
            inner = cleaned_str[1:-1].replace('""', '"').strip()
            try:
                parsed = json.loads(inner)
                if isinstance(parsed, list):
                    return parsed, True
            except Exception:
                pass
                
        # 3. Repair: Python AST literal eval (for single-quoted JSON: [{'sku': '...'}])
        try:
            parsed = ast.literal_eval(cleaned_str)
            if isinstance(parsed, list):
                return parsed, True
        except Exception:
            pass
            
        # 4. Repair: Single quotes to double quotes replacement
        if "'" in cleaned_str and '"' not in cleaned_str:
            try:
                replaced = cleaned_str.replace("'", '"')
                parsed = json.loads(replaced)
                if isinstance(parsed, list):
                    return parsed, True
            except Exception:
                pass
                
        return [], False
        
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned = record.copy()
        corrections = []
        
        items_raw = cleaned.get("items_json")
        delivery_cost_val = cleaned.get("delivery_cost", "0.0")
        
        # Parse delivery cost
        try:
            delivery_cost = float(str(delivery_cost_val).strip()) if delivery_cost_val else 0.0
            if delivery_cost < 0:
                delivery_cost = abs(delivery_cost)
                cleaned["delivery_cost"] = str(delivery_cost)
                corrections.append({
                    "field": "delivery_cost",
                    "original_value": delivery_cost_val,
                    "corrected_value": str(delivery_cost),
                    "rule_code": "DELIVERY_COST_CORRECTION"
                })
        except ValueError:
            delivery_cost = 0.0
            
        if isinstance(items_raw, str) and items_raw.strip():
            items, was_repaired = self._try_parse_items_json(items_raw)
            
            if items and isinstance(items, list) and len(items) > 0:
                items_changed = was_repaired
                items_total_sum = 0.0
                
                for idx, item in enumerate(items):
                    if not isinstance(item, dict):
                        continue
                        
                    def clean_numeric(val):
                        if isinstance(val, (int, float)):
                            return float(val)
                        if not isinstance(val, str):
                            return 0.0
                        arabic_digits = "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫"
                        latin_digits = "01234567890123456789."
                        trans_table = str.maketrans(arabic_digits, latin_digits)
                        cleaned_num = val.translate(trans_table).replace(",", "").strip()
                        try:
                            return float(cleaned_num)
                        except ValueError:
                            return 0.0

                    qty = clean_numeric(item.get("qty", 0))
                    unit_price = clean_numeric(item.get("unit_price", 0.0))
                    total = clean_numeric(item.get("total", 0.0))
                    
                    # Fix negative quantity if the total is positive and matches absolute quantity
                    if qty < 0 and (total > 0 or unit_price > 0):
                        original_qty = item.get("qty")
                        qty = abs(qty)
                        item["qty"] = int(qty) if qty.is_integer() else qty
                        items_changed = True
                        corrections.append({
                            "field": f"items_json[{idx}].qty",
                            "original_value": original_qty,
                            "corrected_value": item["qty"],
                            "rule_code": "NEGATIVE_QTY_CORRECTION"
                        })
                        
                    # Fix negative unit_price if total is positive
                    if unit_price < 0 and total > 0:
                        original_up = item.get("unit_price")
                        unit_price = abs(unit_price)
                        item["unit_price"] = unit_price
                        items_changed = True
                        corrections.append({
                            "field": f"items_json[{idx}].unit_price",
                            "original_value": original_up,
                            "corrected_value": unit_price,
                            "rule_code": "NEGATIVE_UNIT_PRICE_CORRECTION"
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
                    
                # If we modified items inside the list or repaired JSON, dump back to string
                if items_changed:
                    cleaned["items_json"] = json.dumps(items, ensure_ascii=False)
                    if was_repaired:
                        corrections.append({
                            "field": "items_json",
                            "original_value": items_raw,
                            "corrected_value": cleaned["items_json"],
                            "rule_code": "JSON_STRUCTURE_REPAIR"
                        })
                    
                # Re-verify and correct total_amount
                expected_total = items_total_sum + delivery_cost
                total_amount_val = cleaned.get("total_amount")
                
                try:
                    total_amount = float(str(total_amount_val).strip()) if total_amount_val is not None else None
                except (ValueError, TypeError):
                    total_amount = None
                    
                # Recover missing / invalid total_amount or correct mismatched total
                if total_amount is None or abs(total_amount - expected_total) > 0.01:
                    cleaned["total_amount"] = str(expected_total)
                    corrections.append({
                        "field": "total_amount",
                        "original_value": total_amount_val,
                        "corrected_value": str(expected_total),
                        "rule_code": self.rule_code
                    })
                    
                # Also correct payment_amount if payment_status is "تم الدفع" and it mismatches or is invalid
                payment_status = cleaned.get("payment_status")
                payment_amount_val = cleaned.get("payment_amount")
                try:
                    payment_amount = float(str(payment_amount_val).strip()) if payment_amount_val is not None else None
                except (ValueError, TypeError):
                    payment_amount = None
                    
                if payment_status == "تم الدفع":
                    if payment_amount is None or abs(payment_amount - expected_total) > 0.01:
                        cleaned["payment_amount"] = str(expected_total)
                        corrections.append({
                            "field": "payment_amount",
                            "original_value": payment_amount_val,
                            "corrected_value": str(expected_total),
                            "rule_code": "PAYMENT_AMOUNT_RECALC"
                        })
                elif payment_status == "بانتظار الدفع" and payment_amount is None:
                    cleaned["payment_amount"] = "0.0"
                    corrections.append({
                        "field": "payment_amount",
                        "original_value": payment_amount_val,
                        "corrected_value": "0.0",
                        "rule_code": "PAYMENT_AMOUNT_RECALC"
                    })
                    
        return cleaned, corrections

