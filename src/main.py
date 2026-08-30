import argparse
import os
import sys
import uuid
from datetime import datetime
from config import settings
from src.file_router import route_file
from src.elt_pipeline import run_elt_pipeline
from src.metrics import save_run_metrics

def process_single_file(file_path: str, incremental: bool = False):
    """Processes a single CSV file through the hybrid ELT data pipeline."""
    # 1. Setup metadata
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"run_{run_timestamp}_{uuid.uuid4().hex[:6]}"
    
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    
    # 2. File Routing (Engine Selection)
    engine = route_file(file_path)
    print(f"\n>>> Starting execution of run '{run_id}' for file '{os.path.basename(file_path)}' using engine '{engine.upper()}'...")
    if incremental:
        print("    Running in INCREMENTAL loading mode (Path B).")
        
    # 3. Execute Pipeline
    try:
        metrics = run_elt_pipeline(
            file_path=file_path,
            run_id=run_id,
            engine=engine,
            file_size_mb=file_size_mb,
            incremental=incremental
        )
        
        # 4. Save and Report Metrics
        if metrics:
            save_run_metrics(metrics)
            print(f"Pipeline run for '{os.path.basename(file_path)}' completed successfully.")
            print(f"  Ingested raw records: {metrics.get('raw_loaded')}")
            print(f"  Valid records (ready): {metrics.get('valid_count')}")
            print(f"  Corrected records: {metrics.get('corrected_count')}")
            print(f"  Quarantined records (failed): {metrics.get('quarantine_count')}")
            print(f"  Time taken: {metrics.get('elapsed_seconds'):.2f} seconds")
            print(f"  Throughput: {metrics.get('throughput'):.1f} rec/sec")
            print(f"  DB updates: Inserts={metrics.get('inserted_count')}, Updates={metrics.get('updated_count')}, Unchanged={metrics.get('unchanged_count')}\n")
            return metrics
        else:
            print(f"Pipeline for '{os.path.basename(file_path)}' completed, but no metrics were generated.")
            return None
            
    except KeyboardInterrupt:
        print(f"\nPipeline interrupted by user during '{file_path}'.")
        sys.exit(130)
    except Exception as e:
        print(f"\nPipeline execution failed for '{file_path}': {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    parser = argparse.ArgumentParser(description="Hybrid ELT Data Pipeline (Python Batch & PySpark)")
    parser.add_argument(
        "--input", 
        default="data", 
        help="Path to input CSV file or directory containing CSV files (default: data)"
    )
    parser.add_argument(
        "--incremental", 
        action="store_true", 
        help="Run in incremental mode with version handling (Path B)"
    )
    
    args = parser.parse_args()
    target_path = args.input
    
    if not os.path.exists(target_path):
        print(f"Error: Target path '{target_path}' does not exist.")
        sys.exit(1)
        
    # Check if target is a directory
    if os.path.isdir(target_path):
        # Discover all CSV files in directory
        csv_files = sorted([
            os.path.join(target_path, f)
            for f in os.listdir(target_path)
            if f.endswith(".csv") and not f.startswith(".")
        ])
        
        if not csv_files:
            print(f"No CSV files found in directory '{target_path}'.")
            sys.exit(0)
            
        print("=" * 60)
        print(f"Discovered {len(csv_files)} CSV file(s) in directory '{target_path}':")
        for f in csv_files:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            print(f"  - {os.path.basename(f)} ({size_mb:.2f} MB)")
        print("=" * 60)
        
        for csv_file in csv_files:
            process_single_file(csv_file, args.incremental)
            
        print("=" * 60)
        print(f"All {len(csv_files)} file(s) processed.")
        print("=" * 60)
    else:
        # Process single file
        process_single_file(target_path, args.incremental)

if __name__ == "__main__":
    main()
