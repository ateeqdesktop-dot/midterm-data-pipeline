import os
from pathlib import Path

# Ensure SPARK_LOCAL_IP is bound to localhost to prevent docker0 interface collision on Linux
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"

# Ensure reports directory exists
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Engine Threshold
# File size <= 200 MB will use Python Batch, otherwise PySpark
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200.0"))

# Python Batch Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "midterm_db")

COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"

# Spark Configuration
SPARK_APP_NAME = "MidtermDataPipeline"
# By default local[*], can be configured to spark://master-ip:7077 for cluster mode
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
# MongoDB Spark Connector configuration
MONGO_SPARK_INPUT_URI = f"{MONGO_URI}/{MONGO_DB_NAME}.{COLLECTION_RAW}"
MONGO_SPARK_OUTPUT_URI = f"{MONGO_URI}/{MONGO_DB_NAME}.{COLLECTION_RAW}"

# Business Validation Constants
VALID_STATUSES = {
    "مؤكد": "مؤكد",
    "قيد الانتظار": "قيد الانتظار",
    "ملغي": "ملغي",
    "مرتجع": "مرتجع",
    "مكتمل": "مكتمل",
    "شحن": "شحن",
    "تم الشحن": "شحن",
    "توصيل": "توصيل",
    "تم التوصيل": "توصيل",
    "بانتظار الدفع": "بانتظار الدفع",
    "تم الدفع": "تم الدفع",
    "بانتظار": "قيد الانتظار",
    "مرفوض": "ملغي"
}

VALID_CITIES = {
    "صنعاء", "عدن", "تعز", "الحديدة", "إب", "ذمار", "المكلا", "سيئون",
    "عمران", "صعدة", "حجة", "البيضاء", "مأرب", "الجوف", "شبوة", "المهرة",
    "سقطرى", "أبين", "لحج", "ضالع", "ريمة", "المحويت"
}

VALID_PAYMENT_METHODS = {
    "نقد عند الاستلام", "محفظة إلكترونية", "بطاقة ائتمان", "حوالة مصرفية", "كاش", "نقد"
}

VALID_CURRENCIES = {"YER", "USD", "SAR"}

# Standard target currency for amounts
TARGET_CURRENCY = "YER"
# Exchange rates to YER (placeholder/logical values)
EXCHANGE_RATES = {
    "YER": 1.0,
    "USD": 600.0,  # approximate rate
    "SAR": 160.0   # approximate rate
}

DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%Y/%m/%d"
]
