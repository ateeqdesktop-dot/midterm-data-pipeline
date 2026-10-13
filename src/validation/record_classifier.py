from typing import Dict, Any, List, Tuple, Set
from src.validation.quarantine_classifier import classify_quarantine_errors

def classify_and_tag_record(
    cleaned_record: Dict[str, Any],
    raw_record: Dict[str, Any],
    corrections: List[Dict[str, Any]],
    seen_order_ids: Set[str]
) -> Tuple[Dict[str, Any], str]:
    """
    Classifies a cleaned record as 'valid', 'corrected', or 'quarantined'.
    Enriches the record dictionary with final quality statuses and error lists.
    Returns:
        Tuple[enriched_record, status]
    """
    record = cleaned_record.copy()
    
    # 1. Run quarantine checks
    error_codes, error_details = classify_quarantine_errors(record, seen_order_ids)
    
    # 2. Add to seen order IDs if order_id is present and not duplicated
    order_id = record.get("order_id")
    if order_id:
        seen_order_ids.add(str(order_id).strip())
        
    # 3. Classify status
    if error_codes:
        status = "quarantined"
        record["quality_status"] = "quarantined"
        record["error_codes"] = error_codes
        record["error_details"] = error_details
        # Quarantine records must contain the raw record for debuggability (section 6.9)
        record["raw_record"] = raw_record
    else:
        if corrections:
            status = "corrected"
            record["quality_status"] = "corrected"
            record["corrections"] = corrections
        else:
            status = "valid"
            record["quality_status"] = "valid"
            
    return record, status
