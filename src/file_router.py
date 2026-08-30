import os
from config import settings

def route_file(file_path: str) -> str:
    """
    Inspects file size and selects the execution engine: python_batch or pyspark.
    Prints metadata and explanation of the decision.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found at path: {file_path}")
        
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    
    print("-" * 50)
    print(f"File Routing Inspection:")
    print(f"  File Path: {file_path}")
    print(f"  File Size: {file_size_mb:.2f} MB")
    print(f"  Threshold: {settings.SMALL_FILE_THRESHOLD_MB:.2f} MB")
    
    if file_size_mb <= settings.SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = (
            f"File size ({file_size_mb:.2f} MB) is below or equal to the threshold "
            f"({settings.SMALL_FILE_THRESHOLD_MB:.2f} MB). Choosing Python Batch engine "
            "to avoid Spark cluster startup overhead and process efficiently using streaming."
        )
    else:
        engine = "pyspark"
        reason = (
            f"File size ({file_size_mb:.2f} MB) exceeds the threshold "
            f"({settings.SMALL_FILE_THRESHOLD_MB:.2f} MB). Choosing PySpark engine "
            "for parallelized processing across partitions and cluster-native scalability."
        )
        
    print(f"  Selected Engine: {engine.upper()}")
    print(f"  Reason: {reason}")
    print("-" * 50)
    
    return engine
