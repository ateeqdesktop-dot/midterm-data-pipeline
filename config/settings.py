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
    # Confirmed statuses
    "مؤكد": "مؤكد",
    "تم التأكيد": "مؤكد",
    "تم التاكيد": "مؤكد",
    "مقبول": "مؤكد",
    
    # Pending statuses
    "قيد الانتظار": "قيد الانتظار",
    "بانتظار": "قيد الانتظار",
    "معلق": "قيد الانتظار",
    "انتظار": "قيد الانتظار",
    "تحت المراجعة": "قيد الانتظار",
    "جديد": "قيد الانتظار",
    
    # Cancelled / Rejected statuses
    "ملغي": "ملغي",
    "ملغى": "ملغي",
    "إلغاء": "ملغي",
    "الغاء": "ملغي",
    "مرفوض": "ملغي",
    "مرفوضة": "ملغي",
    
    # Returned statuses
    "مرتجع": "مرتجع",
    "إرجاع": "مرتجع",
    "ارجاع": "مرتجع",
    "مسترجع": "مرتجع",
    "معاد": "مرتجع",
    
    # Completed / Delivered statuses
    "مكتمل": "مكتمل",
    "مكتملة": "مكتمل",
    "توصيل": "توصيل",
    "تم التوصيل": "توصيل",
    "تم التسليم": "توصيل",
    "تم التسليم بنجاح": "توصيل",
    "تم الاستلام": "توصيل",
    "منجز": "توصيل",
    
    # Shipping statuses
    "شحن": "شحن",
    "تم الشحن": "شحن",
    "قيد الشحن": "شحن",
    "جاري التوصيل": "شحن",
    "في الطريق": "شحن",
    "مشحون": "شحن",
    
    # Payment Statuses
    "بانتظار الدفع": "بانتظار الدفع",
    "قيد الدفع": "بانتظار الدفع",
    "معلق الدفع": "بانتظار الدفع",
    "غير مدفوع": "بانتظار الدفع",
    "بانتظار السداد": "بانتظار الدفع",
    "أجل": "بانتظار الدفع",
    "اجل": "بانتظار الدفع",
    
    "تم الدفع": "تم الدفع",
    "مدفوع": "تم الدفع",
    "مدفوعة": "تم الدفع",
    "مسدد": "تم الدفع",
    "تم السداد": "تم الدفع",
    "خالص": "تم الدفع"
}

VALID_CITIES = {
    "صنعاء", "عدن", "تعز", "الحديدة", "إب", "ذمار", "المكلا", "سيئون",
    "عمران", "صعدة", "حجة", "البيضاء", "مأرب", "الجوف", "شبوة", "المهرة",
    "سقطرى", "أبين", "لحج", "الضالع", "ضالع", "ريمة", "المحويت"
}

CITY_SYNONYMS = {
    "صنعا": "صنعاء",
    "الحديده": "الحديدة",
    "حديدة": "الحديدة",
    "حديده": "الحديدة",
    "حجه": "حجة",
    "صعده": "صعدة",
    "اب": "إب",
    "إب ": "إب",
    "مارب": "مأرب",
    "البيضا": "البيضاء",
    "بيضاء": "البيضاء",
    "سيؤن": "سيئون",
    "المكلا ": "المكلا",
    "مكلا": "المكلا",
    "المهره": "المهرة",
    "مهره": "المهرة",
    "سقطري": "سقطرى",
    "ريمه": "ريمة",
    "المحويت ": "المحويت",
    "محويت": "المحويت",
    "ضالع": "الضالع",
    "الضالع": "الضالع"
}

VALID_DISTRICTS = {
    "كريتر", "القاهرة", "حدة", "خور مكسر", "جبلة", "التحرير", "الحصبة", "شعوب",
    "المعلا", "الشيخ عثمان", "المنصورة", "التواهي", "الروضة", "السبعين", "معين",
    "بني الحارث", "الصافية", "أزال", "المشنة", "صالة", "المظفر", "الميناء"
}

DISTRICT_SYNONYMS = {
    "حده": "حدة",
    "الحصبه": "الحصبة",
    "حصبه": "الحصبة",
    "القاهره": "القاهرة",
    "قاهرة": "القاهرة",
    "قاهره": "القاهرة",
    "جبله": "جبلة",
    "خورمكسر": "خور مكسر",
    "خور المكسر": "خور مكسر",
    "تحرير": "التحرير",
    "معلا": "المعلا",
    "شيخ عثمان": "الشيخ عثمان",
    "منصورة": "المنصورة",
    "منصوره": "المنصورة",
    "المنصوره": "المنصورة",
    "تواهي": "التواهي",
    "روضة": "الروضة",
    "روضه": "الروضة",
    "الروضه": "الروضة",
    "سبعين": "السبعين",
    "بني حارث": "بني الحارث",
    "صافية": "الصافية",
    "ازال": "أزال",
    "مشاحنة": "المشنة",
    "مشنة": "المشنة",
    "مظفر": "المظفر",
    "ميناء": "الميناء"
}

VALID_DELIVERY_TYPES = {"سريع", "عادي"}

DELIVERY_TYPE_SYNONYMS = {
    "سريع": "سريع",
    "توصيل سريع": "سريع",
    "مستعجل": "سريع",
    "إكسبرس": "سريع",
    "اكسبرس": "سريع",
    "express": "سريع",
    "fast": "سريع",
    
    "عادي": "عادي",
    "توصيل عادي": "عادي",
    "قياسي": "عادي",
    "standard": "عادي",
    "normal": "عادي",
    "regular": "عادي"
}

VALID_PAYMENT_METHODS = {
    "نقد عند الاستلام", "نقدًا عند التسليم", "نقد عند التسليم", "نقد", "كاش", 
    "محفظة إلكترونية", "محفظة", "بطاقة ائتمان", "بطاقة", "حوالة مصرفية", "حوالة"
}

PAYMENT_METHOD_SYNONYMS = {
    "نقد عند الاستلام": "نقد عند الاستلام",
    "نقدًا عند التسليم": "نقد عند الاستلام",
    "نقد عند التسليم": "نقد عند الاستلام",
    "دفع عند الاستلام": "نقد عند الاستلام",
    "كاش": "نقد عند الاستلام",
    "نقد": "نقد عند الاستلام",
    "cash": "نقد عند الاستلام",
    "cod": "نقد عند الاستلام",
    
    "بطاقة ائتمان": "بطاقة ائتمان",
    "بطاقة": "بطاقة ائتمان",
    "بطاقه": "بطاقة ائتمان",
    "فيزا": "بطاقة ائتمان",
    "ماستركارد": "بطاقة ائتمان",
    "بطاقة بنكية": "بطاقة ائتمان",
    "card": "بطاقة ائتمان",
    "credit_card": "بطاقة ائتمان",
    
    "محفظة إلكترونية": "محفظة إلكترونية",
    "محفظة الكترونية": "محفظة إلكترونية",
    "محفظه إلكترونيه": "محفظة إلكترونية",
    "محفظة": "محفظة إلكترونية",
    "محفظه": "محفظة إلكترونية",
    "كريمي جوال": "محفظة إلكترونية",
    "جوالي": "محفظة إلكترونية",
    "ون كاش": "محفظة إلكترونية",
    "بيس": "محفظة إلكترونية",
    "فلوسك": "محفظة إلكترونية",
    "wallet": "محفظة إلكترونية",
    
    "حوالة مصرفية": "حوالة مصرفية",
    "حوالة": "حوالة مصرفية",
    "حواله": "حوالة مصرفية",
    "تحويل بنكي": "حوالة مصرفية",
    "حوالة مالية": "حوالة مصرفية",
    "transfer": "حوالة مصرفية"
}


CURRENCY_SYNONYMS = {
    "ريال يمني": "YER",
    "ريال": "YER",
    "لاير يمني": "YER",
    "لاير": "YER",
    "يمني": "YER",
    "ر.ي": "YER",
    "ريالات": "YER",
    "YER": "YER",
    "yer": "YER",
    "USD": "USD",
    "usd": "USD",
    "دولار": "USD",
    "دولار أمريكي": "USD",
    "دولار امريكي": "USD",
    "$": "USD",
    "SAR": "SAR",
    "sar": "SAR",
    "ريال سعودي": "SAR",
    "سعودي": "SAR",
    "ر.س": "SAR"
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
    # ISO-like formats
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M",
    
    # Dash-separated formats (with time)
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
    "%m-%d-%Y %H:%M:%S",
    "%m-%d-%Y %H:%M",
    
    # Slash-separated formats (with time)
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    
    # Date only formats
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    
    # Dot-separated formats
    "%Y.%m.%d %H:%M:%S",
    "%d.%m.%Y %H:%M:%S",
    "%Y.%m.%d",
    "%d.%m.%Y",
    
    # 12-hour AM/PM formats
    "%Y-%m-%d %I:%M:%S %p",
    "%d-%m-%Y %I:%M:%S %p",
    "%d/%m/%Y %I:%M:%S %p",
    "%Y/%m/%d %I:%M:%S %p",
    "%Y-%m-%d %I:%M %p",
    "%d-%m-%Y %I:%M %p",
    "%d/%m/%Y %I:%M %p",
    "%Y/%m/%d %I:%M %p"
]
