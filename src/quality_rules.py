"""Deterministic order cleaning, correction, and quarantine rules."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
PRICE_WORDS = {"ألفان": "2000", "الفان": "2000", "خمسة آلاف": "5000", "خمسه الاف": "5000"}
STATUS_MAP = {"مؤكد": "confirmed", "مؤكدة": "confirmed", "مدفوع": "paid", "مسافات": ""}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _add(
    corrections: list[dict[str, str]], field: str, original: Any, corrected: Any, rule: str
) -> None:
    if str(original) != str(corrected):
        corrections.append(
            {
                "field": field,
                "original_value": str(original),
                "corrected_value": str(corrected),
                "rule_code": rule,
            }
        )


def clean_order(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    corrections: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []

    for key, value in list(row.items()):
        if isinstance(value, str):
            trimmed = value.strip()
            _add(corrections, key, value, trimmed, "TRIM_WHITESPACE")
            row[key] = trimmed

    order_id = _text(row.get("order_id"))
    customer_id = _text(row.get("customer_id"))
    if not order_id:
        errors.append({"code": "MISSING_ORDER_ID", "detail": "order_id is required"})
    if not customer_id:
        errors.append({"code": "MISSING_CUSTOMER_ID", "detail": "customer_id is required"})

    if order_id and order_id != _text(row.get("order_id")):
        _add(corrections, "order_id", row.get("order_id"), order_id, "TRIM_ORDER_ID")
    row["order_id"] = order_id
    row["customer_id"] = customer_id

    for field in ("price", "quantity", "shipping_cost"):
        if field not in row:
            continue
        original = row[field]
        value = _text(original).translate(ARABIC_DIGITS).replace(",", "")
        for word, number in PRICE_WORDS.items():
            value = value.replace(word, number)
        value = re.sub(r"(?i)(ريال يمني|ريال|لاير يمني|لاير|yer|usd|\$)", "", value).strip()
        if field == "price" and value and value != _text(original):
            _add(corrections, field, original, value, "NORMALIZE_CURRENCY_AND_DIGITS")
        try:
            number = Decimal(value) if value else None
        except InvalidOperation:
            number = None
        if number is None:
            if field == "price":
                errors.append({"code": "UNKNOWN_PRICE", "detail": "price is not inferable"})
        elif number < 0:
            errors.append({"code": "AMBIGUOUS_NEGATIVE_VALUE", "detail": f"negative {field}"})
        else:
            normalized = float(number) if number % 1 else int(number)
            _add(corrections, field, original, normalized, "PARSE_NUMERIC_VALUE")
            row[field] = normalized

    if "phone" in row:
        original = row["phone"]
        value = _text(original).translate(ARABIC_DIGITS)
        value = re.sub(r"[\s()\-]", "", value)
        _add(corrections, "phone", original, value, "NORMALIZE_PHONE")
        row["phone"] = value

    if "customer_email" in row:
        original = row["customer_email"]
        value = re.sub(r"@+", "@", _text(original)).replace("..", ".")
        _add(corrections, "customer_email", original, value, "EMAIL_REPEATED_SYMBOLS")
        if value.count("@") != 1 or "." not in value.split("@")[-1]:
            errors.append({"code": "INVALID_EMAIL", "detail": "email remains invalid"})
        row["customer_email"] = value

    if "order_date" in row:
        original = row["order_date"]
        value = _text(original)
        parsed = None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                parsed = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
        if parsed is None:
            errors.append(
                {"code": "INVALID_IMPOSSIBLE_DATE", "detail": "date is invalid or impossible"}
            )
        else:
            normalized = parsed.strftime("%Y-%m-%d")
            _add(corrections, "order_date", original, normalized, "NORMALIZE_DATE")
            row["order_date"] = normalized

    for field in ("status", "payment_status"):
        if field in row:
            original = row[field]
            normalized = STATUS_MAP.get(_text(original), _text(original).lower())
            _add(corrections, field, original, normalized, "NORMALIZE_SYNONYM")
            row[field] = normalized

    items_value = row.get("items", "")
    try:
        items = json.loads(items_value) if isinstance(items_value, str) else items_value
    except json.JSONDecodeError:
        items = None
    if not isinstance(items, list):
        errors.append({"code": "CORRUPTED_ITEMS_JSON", "detail": "items must be a JSON list"})
    elif not items:
        errors.append({"code": "EMPTY_ITEMS", "detail": "order has no items"})
    else:
        row["items"] = items

    if row.get("price") is not None and isinstance(row.get("items"), list):
        item_total = sum(
            Decimal(str(item.get("price", 0))) * Decimal(str(item.get("quantity", 1)))
            for item in row["items"]
            if isinstance(item, dict)
        )
        shipping = Decimal(str(row.get("shipping_cost", 0) or 0))
        expected = item_total + shipping
        if "total" in row:
            original = row["total"]
            try:
                current = Decimal(_text(original).translate(ARABIC_DIGITS).replace(",", ""))
            except InvalidOperation:
                current = Decimal(-1)
            if current != expected:
                _add(corrections, "total", original, float(expected), "RECALCULATE_ORDER_TOTAL")
                row["total"] = float(expected)

    if len(errors) > 1:
        errors = [
            {"code": "MULTIPLE_CONFLICTING_ERRORS", "detail": "; ".join(e["code"] for e in errors)}
        ]
    status = "quarantined" if errors else ("corrected" if corrections else "valid")
    row["quality_status"] = status
    row["corrections"] = corrections
    row["error_codes"] = [e["code"] for e in errors]
    row["error_details"] = errors
    return row
