import json
import os
from typing import Dict, Any
from config import settings

def save_run_metrics(metrics: Dict[str, Any]) -> str:
    """
    Saves execution metrics to reports/results.json.
    Validates the consistency equation: raw_loaded == valid + corrected + quarantine.
    """
    reports_file = settings.REPORTS_DIR / "results.json"
    
    # 1. Consistency Verification (Section 6.11)
    raw_loaded = metrics.get("raw_loaded", 0)
    valid_count = metrics.get("valid_count", 0)
    corrected_count = metrics.get("corrected_count", 0)
    quarantine_count = metrics.get("quarantine_count", 0)
    
    computed_sum = valid_count + corrected_count + quarantine_count
    
    print("-" * 50)
    print("Consistency Checking (Rule 6.11):")
    print(f"  Raw loaded count: {raw_loaded}")
    print(f"  Valid ({valid_count}) + Corrected ({corrected_count}) + Quarantine ({quarantine_count}) = {computed_sum}")
    
    if raw_loaded == computed_sum:
        print("  [SUCCESS] Consistency Equation holds: raw_loaded == valid + corrected + quarantine.")
        metrics["consistency_valid"] = True
    else:
        print("  [WARNING] Consistency Equation MISMATCH! Some records may have been lost or duplicated.")
        metrics["consistency_valid"] = False
        
    # 2. Append metrics to the JSON file
    existing_runs = []
    if os.path.exists(reports_file):
        try:
            with open(reports_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    existing_runs = json.loads(content)
                    if not isinstance(existing_runs, list):
                        existing_runs = [existing_runs]
        except Exception as e:
            print(f"  Could not read existing reports file: {e}. Resetting file.")
            
    existing_runs.append(metrics)
    
    try:
        with open(reports_file, 'w', encoding='utf-8') as f:
            json.dump(existing_runs, f, indent=2, ensure_ascii=False)
        print(f"Metrics saved successfully to '{reports_file}'.")
    except Exception as e:
        print(f"  [ERROR] Failed to save metrics file: {e}")
        
    print("-" * 50)
    return str(reports_file)
